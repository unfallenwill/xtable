use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ak = std::env::var("TOS_AK")?;
    let sk = std::env::var("TOS_SK")?;
    let endpoint = std::env::var("TOS_ENDPOINT")
        .unwrap_or_else(|_| "https://tos-s3-cn-beijing.volces.com".to_string());
    let bucket = std::env::var("TOS_BUCKET").unwrap_or_else(|_| "test-xtable".to_string());

    let creds = Credentials::new(ak.clone(), sk.clone(), None, None, "probe");
    let cfg = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(&endpoint)
        .region(Region::new("cn-beijing".to_string()))
        .credentials_provider(creds)
        .load()
        .await;
    let client = Client::from_conf(
        aws_sdk_s3::config::Builder::from(&cfg)
            .force_path_style(false) // TOS prefers virtual-hosted
            .build(),
    );

    println!("=== ListBuckets ===");
    match client.list_buckets().send().await {
        Ok(out) => {
            println!("OK");
            for b in out.buckets() {
                println!("  bucket: {}", b.name().unwrap_or_default());
            }
        }
        Err(e) => {
            let s = format!("{:?}", e);
            println!("ERR: {}", s.lines().next().unwrap_or(""));
        }
    }

    println!("\n=== ListObjectsV2 in {bucket} ===");
    match client.list_objects_v2().bucket(&bucket).send().await {
        Ok(out) => {
            let objects = out.contents();
            println!("OK ({} objects)", objects.len());
            for o in objects {
                println!("  obj: {} (size={})", o.key().unwrap_or_default(), o.size().unwrap_or(0));
            }
        }
        Err(e) => {
            let s = format!("{:?}", e);
            println!("ERR: {}", s.lines().next().unwrap_or(""));
        }
    }

    let k = format!("xtable-probe-{}", std::process::id());
    println!("\n=== PutObject {k} ===");
    match client
        .put_object()
        .bucket(&bucket)
        .key(&k)
        .body(ByteStream::from_static(b"hello-tos-from-xtable-2026"))
        .content_type("text/plain")
        .send()
        .await
    {
        Ok(out) => println!("OK, etag={:?}", out.e_tag()),
        Err(e) => {
            let s = format!("{:?}", e);
            println!("ERR: {}", s.lines().next().unwrap_or(""));
        }
    }

    println!("\n=== GetObject {k} ===");
    match client.get_object().bucket(&bucket).key(&k).send().await {
        Ok(out) => {
            let body = out.body.collect().await?.into_bytes();
            println!("OK, body={:?}", String::from_utf8_lossy(&body));
        }
        Err(e) => {
            let s = format!("{:?}", e);
            println!("ERR: {}", s.lines().next().unwrap_or(""));
        }
    }

    println!("\n=== DeleteObject {k} ===");
    match client.delete_object().bucket(&bucket).key(&k).send().await {
        Ok(_) => println!("OK"),
        Err(e) => {
            let s = format!("{:?}", e);
            println!("ERR: {}", s.lines().next().unwrap_or(""));
        }
    }

    println!("\n=== HeadBucket {bucket} ===");
    match client.head_bucket().bucket(&bucket).send().await {
        Ok(_) => println!("OK"),
        Err(e) => {
            let s = format!("{:?}", e);
            println!("ERR: {}", s.lines().next().unwrap_or(""));
        }
    }

    Ok(())
}