use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use log::{info, warn};
use reqwest::StatusCode;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{Read, Write};

use std::sync::{Arc, Mutex};
use warp::http::Response;
use warp::path::Tail;
use warp::Filter;

use borsh::{BorshDeserialize, BorshSerialize};
use reqwest::header::REFERER;

const MAGIC_CACHE_DURATION_SECONDS: i64 = 1 * 60 * 60;
const REGULAR_CACHE_DURATION_SECONDS: i64 = 30 * 24 * 60 * 60;
const MAX_REFRESH_TIMEOUT: u64 = 60 * 60;
const PURGE_MAGIC_KEYWORD: &str = "purge";

#[derive(Debug, PartialEq, Copy, Clone, Eq, Hash, BorshSerialize, BorshDeserialize)]
pub enum ImgType {
    Thumbnail,
    Large,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct Image {
    content_type: String,
    body: Vec<u8>,
}

pub struct ImageWithCacheDuration {
    pub image: Image,
    pub cache_duration_seconds: i64,
}

#[derive(Debug, Clone)]
pub enum FetchError {
    InvalidRescaleType,
    RequestFailed,
    UnsupportedContentType,
    BodyReadFailed,
    TextReadFailed,
    InvalidDataUrl,
    Purge,
    Status(StatusCode),
}

#[derive(Debug, Clone)]
pub enum CachedImage {
    Failed {
        err: FetchError,
        attempts: Vec<DateTime<Utc>>,
    },
    Success {
        image: Image,
        time: DateTime<Utc>,
    },
}

#[derive(Debug, Clone)]
pub enum CachedMagicUrl {
    Failed {
        err: FetchError,
        attempts: Vec<DateTime<Utc>>,
    },
    Success {
        url: String,
        status: u16,
        time: DateTime<Utc>,
    },
}

type ImgPair = (ImgType, String);
type MagicCache = Arc<Mutex<HashMap<String, CachedMagicUrl>>>;
type ImgCache = Arc<Mutex<HashMap<ImgPair, CachedImage>>>;

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct SavedImage {
    pub pair: ImgPair,
    pub image: Image,
    pub time_nanos: i64,
}

pub fn sha256(data: &[u8]) -> [u8; 32] {
    use sha2::Digest;
    sha2::Sha256::digest(data).into()
}

#[tokio::main]
async fn main() {
    if env::var_os("IMAGE_RESCALE_URL_Thumbnail").is_none()
        || env::var_os("IMAGE_RESCALE_URL_Large").is_none()
    {
        panic!("Env IMAGE_RESCALE_URL_Thumbnail and IMAGE_RESCALE_URL_Large are required");
    }
    env_logger::init();

    let imgs: ImgCache = Arc::new(Mutex::new(HashMap::new()));
    let imgs = warp::any().map(move || imgs.clone());

    let magic: MagicCache = Arc::new(Mutex::new(HashMap::new()));
    let magic = warp::any().map(move || magic.clone());

    let cors = warp::cors().allow_any_origin();
    let log = warp::log("warp");

    let proxy =
        warp::path!(String / ..)
            .and(warp::path::tail())
            .and(
                warp::filters::query::raw()
                    .map(|q| Some(q))
                    .or(warp::any().map(|| None))
                    .unify(),
            )
            .and(imgs)
            .and(magic)
            .and_then(
                |img_type,
                 img_path: Tail,
                 query: Option<String>,
                 imgs: ImgCache,
                 magic: MagicCache| async move {
                    let url = if let Some(query) = query {
                        format!("{}?{}", img_path.as_str(), query)
                    } else {
                        img_path.as_str().to_string()
                    };
                    match proxy_img(img_type, url, imgs, magic).await {
                        Ok(ImageWithCacheDuration {
                            image: Image { content_type, body },
                            cache_duration_seconds,
                        }) => Ok(Response::builder()
                            .header("content-type", content_type)
                            .header(
                                "Cache-Control",
                                format!("public,max-age={cache_duration_seconds}"),
                            )
                            .body(body)),
                        Err(e) => match e {
                            FetchError::Purge => Ok(Response::builder()
                                .header("content-type", "text/plain")
                                .body("Purged".as_bytes().to_vec())),
                            _ => Err(warp::reject::reject()),
                        },
                    }
                },
            )
            .with(cors.clone())
            .with(log);

    let port: u16 = env::var_os("PORT")
        .map(|port| port.to_str().unwrap().parse().unwrap())
        .unwrap_or(3030);

    warp::serve(proxy).run(([127, 0, 0, 1], port)).await;
}

async fn proxy_img(
    mut img_type: String,
    mut url: String,
    imgs: ImgCache,
    magic: MagicCache,
) -> Result<ImageWithCacheDuration, FetchError> {
    let is_magic = img_type == "magic";
    if is_magic {
        if let Some((t, u)) = url.clone().split_once("/") {
            img_type = t.to_string();
            url = u.to_string();
        } else {
            return Err(FetchError::InvalidRescaleType);
        }
    }
    if img_type == PURGE_MAGIC_KEYWORD {
        let _magic_url = magic.lock().unwrap().remove(&url);
        return Err(FetchError::Purge);
    }
    let img_type = match img_type.as_str() {
        "thumbnail" => ImgType::Thumbnail,
        "large" => ImgType::Large,
        _ => return Err(FetchError::InvalidRescaleType),
    };
    let mut cache_duration_seconds = REGULAR_CACHE_DURATION_SECONDS;
    if is_magic {
        let (resolved_url, status) = resolve_magic_url(url, magic).await?;
        cache_duration_seconds = if status.as_u16() == 200 {
            MAGIC_CACHE_DURATION_SECONDS
        } else {
            0
        };
        url = resolved_url;
    }

    if url.starts_with("data:image/") {
        let mut parts = url[5..].splitn(2, ",");
        let content_type = parts
            .next()
            .ok_or_else(|| FetchError::InvalidDataUrl)?
            .splitn(2, ";")
            .next()
            .ok_or_else(|| FetchError::InvalidDataUrl)?;
        let body = base64::decode(parts.next().ok_or_else(|| FetchError::InvalidDataUrl)?)
            .map_err(|_| FetchError::InvalidDataUrl)?;
        return Ok(ImageWithCacheDuration {
            image: Image {
                content_type: content_type.to_string(),
                body,
            },
            cache_duration_seconds,
        });
    }

    let pair = (img_type, url);
    let img = imgs.lock().unwrap().get(&pair).cloned();
    let mut attempts = if let Some(img) = img {
        info!(target: "cache", "Retrieving from cache {:?} {}", pair.0, pair.1);
        match img {
            CachedImage::Failed { err, attempts } => {
                let now = Utc::now();
                let num_attempts = attempts.len() as u32;
                warn!(target: "cache", "Failed attempts {}", num_attempts);
                let timeout = Duration::seconds(std::cmp::min(
                    MAX_REFRESH_TIMEOUT,
                    2u64.pow(num_attempts - 1),
                ) as _);
                let duration = now.signed_duration_since(attempts.last().unwrap().clone());
                if duration < timeout {
                    return Err(err);
                }
                attempts
            }
            CachedImage::Success { image, .. } => {
                return Ok(ImageWithCacheDuration {
                    image,
                    cache_duration_seconds,
                })
            }
        }
    } else {
        if let Some(saved_image) = read_from_disk(&pair) {
            info!(target: "cache", "Retrieving from disk {:?} {}", pair.0, pair.1);
            return Ok(ImageWithCacheDuration {
                image: cache_and_return(imgs, saved_image),
                cache_duration_seconds,
            });
        }
        vec![]
    };
    let url = format!(
        "{}/{}",
        env::var_os(format!("IMAGE_RESCALE_URL_{:?}", pair.0))
            .unwrap()
            .to_str()
            .unwrap(),
        pair.1
    );
    let res = fetch_img(url).await;
    info!(target: "cache", "Caching {:?} {}", pair.0, pair.1);
    match res {
        Ok(image) => {
            let saved_image = write_to_disk(pair, image).expect("Failed to save to disk");
            Ok(ImageWithCacheDuration {
                image: cache_and_return(imgs, saved_image),
                cache_duration_seconds,
            })
        }
        Err(err) => {
            attempts.push(Utc::now());
            imgs.lock().unwrap().insert(
                pair,
                CachedImage::Failed {
                    err: err.clone(),
                    attempts,
                },
            );
            Err(err)
        }
    }
}

async fn resolve_magic_url(
    url: String,
    magic: MagicCache,
) -> Result<(String, StatusCode), FetchError> {
    let magic_url = magic.lock().unwrap().get(&url).cloned();
    let attempts = if let Some(magic_url) = magic_url {
        info!(target: "cache", "Retrieving from magic cache {}", url);
        match magic_url {
            CachedMagicUrl::Failed { err, attempts } => {
                let now = Utc::now();
                let num_attempts = attempts.len() as u32;
                warn!(target: "cache", "Failed attempts {}", num_attempts);
                let timeout = Duration::seconds(std::cmp::min(
                    MAX_REFRESH_TIMEOUT,
                    2u64.pow(num_attempts - 1),
                ) as _);
                let duration = now.signed_duration_since(attempts.last().unwrap().clone());
                if duration < timeout {
                    return Err(err);
                }
                attempts
            }
            CachedMagicUrl::Success {
                url: magic_url,
                status,
                time,
            } => {
                let now = Utc::now();
                let duration = now.signed_duration_since(time);
                if duration > Duration::seconds(MAGIC_CACHE_DURATION_SECONDS) {
                    tokio::spawn(async move {
                        let _res = magic_fetch_and_cache(url, magic, vec![]).await;
                    });
                }
                return Ok((magic_url, StatusCode::from_u16(status).unwrap()));
            }
        }
    } else {
        Vec::new()
    };

    magic_fetch_and_cache(url, magic, attempts).await
}

async fn magic_fetch_and_cache(
    url: String,
    magic: MagicCache,
    mut attempts: Vec<DateTime<Utc>>,
) -> Result<(String, StatusCode), FetchError> {
    let res = fetch_magic_url(url.clone()).await;
    info!(target: "cache", "Caching magic {}", url);
    match res {
        Ok((magic_url, status)) => {
            let time = Utc::now();
            magic.lock().unwrap().insert(
                url,
                CachedMagicUrl::Success {
                    url: magic_url.clone(),
                    status: status.as_u16(),
                    time,
                },
            );
            Ok((magic_url, status))
        }
        Err(err) => {
            attempts.push(Utc::now());
            magic.lock().unwrap().insert(
                url,
                CachedMagicUrl::Failed {
                    err: err.clone(),
                    attempts,
                },
            );
            Err(err)
        }
    }
}

fn cache_and_return(imgs: ImgCache, saved_image: SavedImage) -> Image {
    let naive = NaiveDateTime::from_timestamp_opt(
        saved_image.time_nanos / 1_000_000_000,
        (saved_image.time_nanos % 1_000_000_000) as u32,
    )
    .unwrap();
    let time = DateTime::from_utc(naive, Utc);
    imgs.lock().unwrap().insert(
        saved_image.pair,
        CachedImage::Success {
            image: saved_image.image.clone(),
            time,
        },
    );
    saved_image.image
}

async fn fetch_img(url: String) -> Result<Image, FetchError> {
    info!(target: "fetch", "Fetching {}", url);
    let client = reqwest::Client::new();
    let response = client
        .get(&url)
        .header(REFERER, env::var_os("REFERER").unwrap().to_str().unwrap())
        .send()
        .await
        .map_err(|_e| FetchError::RequestFailed)?;
    if !response.status().is_success() {
        return Err(FetchError::Status(response.status()));
    }
    let content_type = response
        .headers()
        .get("content-type")
        .ok_or_else(|| FetchError::UnsupportedContentType)?
        .to_str()
        .map_err(|_e| FetchError::UnsupportedContentType)?
        .to_string();
    // println!("response {:#?}", response);
    let body = response
        .bytes()
        .await
        .map_err(|_e| FetchError::BodyReadFailed)?;
    Ok(Image {
        content_type,
        body: body.to_vec(),
    })
}

async fn fetch_magic_url(url: String) -> Result<(String, StatusCode), FetchError> {
    info!(target: "fetch", "Fetching magic url {}", url);
    let client = reqwest::Client::new();
    let response = client
        .get(&url)
        .header(REFERER, env::var_os("REFERER").unwrap().to_str().unwrap())
        .send()
        .await
        .map_err(|_e| FetchError::RequestFailed)?;
    let status = response.status();
    if !response.status().is_success() {
        return Err(FetchError::Status(response.status()));
    }
    let content_type = response
        .headers()
        .get("content-type")
        .ok_or_else(|| FetchError::UnsupportedContentType)?
        .to_str()
        .map_err(|_e| FetchError::UnsupportedContentType)?
        .to_string();
    if !content_type.starts_with("text/plain") {
        return Err(FetchError::UnsupportedContentType);
    }
    let text = response
        .text()
        .await
        .map_err(|_e| FetchError::TextReadFailed)?;
    Ok((text, status))
}

fn read_from_disk(pair: &ImgPair) -> Option<SavedImage> {
    let (_dir, path) = pair_to_path(&pair);
    let mut file = match File::open(&path) {
        Ok(file) => file,
        Err(_e) => return None,
    };
    let mut buf = Vec::new();
    if file.read_to_end(&mut buf).is_ok() {
        SavedImage::try_from_slice(&buf).ok()
    } else {
        None
    }
}

fn pair_to_path(pair: &ImgPair) -> (String, String) {
    let filename = hex::encode(sha256(pair.1.as_bytes()));
    let (dir1, filename) = filename.split_at(3);
    let (dir2, filename) = filename.split_at(3);
    let dir = format!("cache/{:?}/{}/{}", pair.0, dir1, dir2);
    let path = format!("{}/{}", dir, filename);
    (dir, path)
}

fn write_to_disk(pair: ImgPair, image: Image) -> Result<SavedImage, std::io::Error> {
    let (dir, path) = pair_to_path(&pair);
    std::fs::create_dir_all(dir)?;
    let mut file = File::create(path).unwrap();
    let saved_image = SavedImage {
        image,
        pair,
        time_nanos: Utc::now().timestamp_nanos(),
    };
    file.write_all(&saved_image.try_to_vec().unwrap())?;
    Ok(saved_image)
}
