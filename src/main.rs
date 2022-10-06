use chrono::{DateTime, Duration, Utc};
use log::{info, warn};
use reqwest::StatusCode;
use std::collections::HashMap;
use std::env;
use std::sync::{Arc, Mutex};
use warp::http::Response;
use warp::path::Tail;
use warp::Filter;

#[derive(Debug, PartialEq, Eq, Hash)]
enum ImgType {
    Square96,
}

#[derive(Debug, Clone)]
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

type ImgCache = Arc<Mutex<HashMap<(ImgType, String), CachedImage>>>;

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
    let log = warp::log("imgs");

    let proxy = warp::path!(String / ..)
        .and(warp::path::tail())
        .and(imgs)
        .and_then(|img_type, img_path: Tail, imgs: ImgCache| async move {
            match proxy_img(img_type, img_path.as_str().to_string(), imgs).await {
                Ok(Image { content_type, body }) => Ok(Response::builder()
                    .header("content-type", content_type)
                    .header("Cache-Control", "public,max-age=2592000")
                    .body(body)),
                Err(_e) => Err(warp::reject::reject()),
            }
        })
        .with(cors.clone())
        .with(log);

    warp::serve(proxy).run(([127, 0, 0, 1], 3034)).await;
}

async fn proxy_img(
    img_type: String,
    img_path: String,
    imgs: ImgCache,
) -> Result<Image, FetchError> {
    let img_type = match img_type.as_str() {
        "96" => ImgType::Square96,
        _ => return Err(FetchError::InvalidRescaleType),
    };
    let pair = (img_type, img_path);
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
            imgs.lock().unwrap().insert(
                pair,
                CachedImage::Success {
                    image: image.clone(),
                    time: Utc::now(),
                },
            );
            Ok(image)
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

async fn fetch_img(url: String) -> Result<Image, FetchError> {
    info!(target: "fetch", "Fetching {}", url);
    let response = reqwest::get(&url)
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
