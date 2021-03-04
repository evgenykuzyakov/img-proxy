use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::{env, io};
use warp::http::Response;
use warp::Filter;

type ImgCache = Arc<Mutex<HashMap<u32, Vec<u8>>>>;

#[tokio::main]
async fn main() {
    if env::var_os("RUST_LOG").is_none() {
        // Set `RUST_LOG=img=debug` to see debug logs,
        // this only shows access logs.
        env::set_var("RUST_LOG", "img=info");
    }
    pretty_env_logger::init();

    // Keep track of all connected users, key is usize, value
    // is an event stream sender.
    let imgs: ImgCache = Arc::new(Mutex::new(HashMap::new()));
    // Turn our "state" into a new Filter...
    let imgs = warp::any().map(move || imgs.clone());

    let cors = warp::cors().allow_any_origin();
    let log = warp::log("img");

    // GET /chat -> messages stream
    let hello = warp::path!(u32)
        .and(imgs)
        .and_then(|img_id, imgs: ImgCache| async move {
            if let Ok(img) = proxy_img(img_id, imgs).await {
                Ok(Response::builder()
                    .header("content-type", "image/png")
                    .header("Cache-Control", "public,max-age=2592000")
                    .body(img))
            } else {
                Err(warp::reject::not_found())
            }
        })
        .with(cors.clone())
        .with(log);

    warp::serve(hello).run(([127, 0, 0, 1], 3032)).await;
}

async fn proxy_img(img_id: u32, imgs: ImgCache) -> Result<Vec<u8>, io::Error> {
    let img = imgs.lock().unwrap().get(&img_id).map(|img| img.clone());
    if let Some(img) = img {
        println!("Retrieving from cache #{}", img_id);
        Ok(img.clone())
    } else {
        let url = format!("https://wayback.berryclub.io/img/{}", img_id);
        println!("Fetching {}", url);
        let body = reqwest::get(&url)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
            .bytes()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        println!("Caching #{}", img_id);
        let img = body.to_vec();
        imgs.lock().unwrap().insert(img_id, img.clone());
        Ok(img)
    }
}
