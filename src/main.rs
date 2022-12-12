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

#[derive(Debug, PartialEq, Copy, Clone, Eq, Hash, BorshSerialize, BorshDeserialize)]
pub enum ImgType {
    Thumbnail,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct Image {
    content_type: String,
    body: Vec<u8>,
}

#[derive(Debug, Clone)]
pub enum FetchError {
    InvalidRescaleType,
    RequestFailed,
    UnsupportedContentType,
    BodyReadFailed,
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


type ImgPair = (ImgType, String);
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
    if env::var_os("IMAGE_RESCALE_URL").is_none() {
        panic!("Env IMAGE_RESCALE_URL is required");
    }
    env_logger::init();

    // Keep track of all connected users, key is usize, value
    // is an event stream sender.
    let imgs: ImgCache = Arc::new(Mutex::new(HashMap::new()));
    // Turn our "state" into a new Filter...
    let imgs = warp::any().map(move || imgs.clone());

    let cors = warp::cors().allow_any_origin();
    let log = warp::log("warp");

    let proxy = warp::path!(String / ..)
        .and(warp::path::tail())
        .and(
            warp::filters::query::raw()
                .map(|q| Some(q))
                .or(warp::any().map(|| None))
                .unify(),
        )
        .and(imgs)
        .and_then(
            |img_type, img_path: Tail, query: Option<String>, imgs: ImgCache| async move {
                let url = if let Some(query) = query {
                    format!("{}?{}", img_path.as_str(), query)
                } else {
                    img_path.as_str().to_string()
                };
                match proxy_img(img_type, url, imgs).await {
                    Ok(Image { content_type, body }) => Ok(Response::builder()
                        .header("content-type", content_type)
                        .header("Cache-Control", "public,max-age=2592000")
                        .body(body)),
                    Err(_e) => Err(warp::reject::reject()),
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

async fn proxy_img(img_type: String, url: String, imgs: ImgCache) -> Result<Image, FetchError> {
    let img_type = match img_type.as_str() {
        "thumbnail" => ImgType::Thumbnail,
        _ => return Err(FetchError::InvalidRescaleType),
    };
    let pair = (img_type, url);
    let img = imgs.lock().unwrap().get(&pair).cloned();
    let mut attempts = if let Some(img) = img {
        info!(target: "cache", "Retrieving from cache {:?} {}", pair.0, pair.1);
        match img {
            CachedImage::Failed { err, attempts } => {
                let now = Utc::now();
                let num_attempts = attempts.len() as u32;
                warn!(target: "cache", "Failed attempts {}", num_attempts);
                let timeout = Duration::seconds(2u64.pow(num_attempts - 1) as _);
                let duration = now.signed_duration_since(attempts.last().unwrap().clone());
                if duration < timeout {
                    return Err(err);
                }
                attempts
            }
            CachedImage::Success { image, .. } => return Ok(image),
        }
    } else {
        // TODO: read from disk
        if let Some(saved_image) = read_from_disk(&pair) {
            info!(target: "cache", "Retrieving from disk {:?} {}", pair.0, pair.1);
            return Ok(cache_and_return(imgs, saved_image));
        }
        vec![]
    };
    let url = format!(
        "{}/{}",
        env::var_os("IMAGE_RESCALE_URL").unwrap().to_str().unwrap(),
        pair.1
    );
    let res = fetch_img(url).await;
    info!(target: "cache", "Caching {:?} {}", pair.0, pair.1);
    match res {
        Ok(image) => {
            let saved_image = write_to_disk(pair, image).expect("Failed to save to disk");
            Ok(cache_and_return(imgs, saved_image))
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

fn cache_and_return(imgs: ImgCache, saved_image: SavedImage) -> Image {
    let naive = NaiveDateTime::from_timestamp(saved_image.time_nanos / 1_000_000_000, (saved_image.time_nanos % 1_000_000_000) as u32);
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
    let response = client.get(&url).header(REFERER, env::var_os("REFERER").unwrap().to_str().unwrap()).send()
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
    let saved_image = SavedImage{
        image,
        pair,
        time_nanos: Utc::now().timestamp_nanos(),
    };
    file.write_all(&saved_image.try_to_vec().unwrap())?;
    Ok(saved_image)
}
