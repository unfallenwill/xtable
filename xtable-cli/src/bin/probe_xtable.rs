//! End-to-end probe through xtable-server against a real TOS backend.
//!
//! All operations use raw HTTP with hand-rolled SigV4 (because we need to
//! inject the `x-xtable-txn-id` header which aws-sdk-s3 cannot do).
//! Transactional header is `x-amz-meta-xtable-txn-id`, which xtable-s3 also
//! accepts as an alias for `x-xtable-txn-id`.
//!
//! Reads ALL secrets from env vars; never writes them to disk.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let xtable_ep = std::env::var("XTABLE_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:19000".to_string());
    let xtable_ak = std::env::var("XTABLE_AK")?;
    let xtable_sk = std::env::var("XTABLE_SK")?;
    let bucket = std::env::var("TOS_BUCKET").unwrap_or_else(|_| "test-xtable".to_string());

    println!("xtable endpoint : {}", xtable_ep);
    println!("bucket          : {}", bucket);

    // 1. BeginTxn
    let begin_url = format!("{}/?transactional=begin", xtable_ep);
    let (status, headers, _) =
        raw_signed(&xtable_ep, &xtable_ak, &xtable_sk, "POST", &begin_url, &[], &[]).await?;
    println!("\n[1] BeginTxn      POST {} -> status {}", begin_url, status);
    anyhow::ensure!(status == 200, "BeginTxn failed");
    let txn_id = headers
        .get("x-xtable-txn-id")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("no x-xtable-txn-id header"))?;
    let snapshot = headers
        .get("x-xtable-snapshot-version")
        .cloned()
        .unwrap_or_default();
    println!("    txn_id       = {}", txn_id);
    println!("    snapshot_ver = {}", snapshot);

    let key_a = format!("xtable-e2e-{}/a.txt", std::process::id());
    let key_b = format!("xtable-e2e-{}/b.txt", std::process::id());

    // 2. PUT a (staged via metadata header)
    println!("\n[2] PUT {} (txn staged via x-amz-meta-xtable-txn-id)", key_a);
    let (status, _, body) = s3_raw(
        &xtable_ep,
        &xtable_ak,
        &xtable_sk,
        "PUT",
        &bucket,
        &key_a,
        b"alpha-row\n",
        &[("x-amz-meta-xtable-txn-id", &txn_id)],
    )
    .await?;
    println!("    status={} body={:?}", status, body);
    anyhow::ensure!(status == 200, "PUT a failed: status={} body={:?}", status, body);

    // 3. PUT b
    println!("\n[3] PUT {} (txn staged via x-amz-meta-xtable-txn-id)", key_b);
    let (status, _, body) = s3_raw(
        &xtable_ep,
        &xtable_ak,
        &xtable_sk,
        "PUT",
        &bucket,
        &key_b,
        b"beta-row\n",
        &[("x-amz-meta-xtable-txn-id", &txn_id)],
    )
    .await?;
    println!("    status={} body={:?}", status, body);
    anyhow::ensure!(status == 200, "PUT b failed");

    // 4. GET a read-your-own-writes
    println!("\n[4] GET {} (read-your-own-writes)", key_a);
    let (status, _, body) = s3_raw(
        &xtable_ep,
        &xtable_ak,
        &xtable_sk,
        "GET",
        &bucket,
        &key_a,
        b"",
        &[("x-amz-meta-xtable-txn-id", &txn_id)],
    )
    .await?;
    println!("    status={} body={:?}", status, body);
    anyhow::ensure!(status == 200 && body == "alpha-row\n", "staged read mismatch");

    // 5. CommitTxn
    let commit_url = format!("{}/?transactional=commit", xtable_ep);
    let (status, headers, body) = raw_signed(
        &xtable_ep,
        &xtable_ak,
        &xtable_sk,
        "POST",
        &commit_url,
        &[("x-xtable-txn-id", &txn_id)],
        &[],
    )
    .await?;
    println!("\n[5] CommitTxn      POST {} -> status {} body={:?}", commit_url, status, body);
    anyhow::ensure!(status == 200, "CommitTxn failed");
    let commit_v = headers
        .get("x-xtable-commit-version")
        .cloned()
        .unwrap_or_default();
    println!("    commit_version = {}", commit_v);

    // 6. GET a post-commit
    println!("\n[6] GET {} (post-commit)", key_a);
    let (status, _, body) = s3_raw(
        &xtable_ep, &xtable_ak, &xtable_sk, "GET", &bucket, &key_a, b"",
        &[], // no txn header → reads from backend TOS
    )
    .await?;
    println!("    status={} body={:?}", status, body);
    anyhow::ensure!(status == 200 && body == "alpha-row\n", "expected committed alpha-row, got {:?}", body);

    // 7. GET b post-commit
    println!("\n[7] GET {} (post-commit)", key_b);
    let (status, _, body) = s3_raw(
        &xtable_ep, &xtable_ak, &xtable_sk, "GET", &bucket, &key_b, b"",
        &[],
    )
    .await?;
    println!("    status={} body={:?}", status, body);
    anyhow::ensure!(status == 200 && body == "beta-row\n", "expected committed beta-row, got {:?}", body);

    println!("\n✅ xtable e2e against TOS OK");
    Ok(())
}

/// S3 operation: bucket+key. Returns (status, headers, body).
async fn s3_raw(
    base: &str,
    ak: &str,
    sk: &str,
    method: &str,
    bucket: &str,
    key: &str,
    body: &[u8],
    extra_headers: &[(&str, &str)],
) -> anyhow::Result<(u16, HashMap<String, String>, String)> {
    let url = format!("{}/{}/{}", base, bucket, key);
    raw_signed(base, ak, sk, method, &url, extra_headers, body).await
}

async fn raw_signed(
    _base: &str,
    ak: &str,
    sk: &str,
    method: &str,
    url: &str,
    extra_headers: &[(&str, &str)],
    body: &[u8],
) -> anyhow::Result<(u16, HashMap<String, String>, String)> {
    let stripped = url
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    let (authority, path_qs) = match stripped.find('/') {
        Some(i) => (&stripped[..i], &stripped[i..]),
        None => (stripped, "/"),
    };
    let (path_only, query_only) = match path_qs.find('?') {
        Some(i) => (&path_qs[..i], &path_qs[i + 1..]),
        None => (path_qs, ""),
    };

    let payload_hash = hex::encode(Sha256::digest(body));

    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let amz_date = amzdate_from_unix(now);
    let datestamp = &amz_date[..8];

    let mut headers: Vec<(String, String)> = Vec::new();
    headers.push(("host".to_string(), authority.to_string()));
    headers.push(("x-amz-content-sha256".to_string(), payload_hash.clone()));
    headers.push(("x-amz-date".to_string(), amz_date.clone()));
    for (k, v) in extra_headers {
        headers.push((k.to_lowercase(), v.to_string()));
    }
    headers.sort_by(|a, b| a.0.cmp(&b.0));
    headers.dedup_by(|a, b| a.0 == b.0);

    let mut canonical_headers = String::new();
    let mut signed_headers_list: Vec<String> = Vec::new();
    for (k, v) in &headers {
        canonical_headers.push_str(&format!("{}:{}\n", k, v.trim()));
        signed_headers_list.push(k.clone());
    }
    let signed_headers_canonical = signed_headers_list.join(";");
    let canonical_request = format!(
        "{}\n{}\n{}\n{}{}\n{}",
        method, path_only, query_only, canonical_headers, signed_headers_canonical, payload_hash,
    );
    let canonical_request_hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));

    let scope = format!("{}/us-east-1/s3/aws4_request", datestamp);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        amz_date, scope, canonical_request_hash
    );

    let k_secret = format!("AWS4{}", sk);
    let k_date = hmac_sha256(k_secret.as_bytes(), datestamp.as_bytes());
    let k_region = hmac_sha256(&k_date, b"us-east-1");
    let k_service = hmac_sha256(&k_region, b"s3");
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature = hex::encode(hmac_sha256(&k_signing, string_to_sign.as_bytes()));

    let auth_header = format!(
        "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
        ak, scope, signed_headers_canonical, signature
    );

    let mut wire = format!(
        "{} {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n",
        method, path_qs, authority
    );
    // Send canonical (signed) headers in their sorted order.
    for (k, v) in &headers {
        wire.push_str(&format!("{}: {}\r\n", k, v));
    }
    // Send extra headers not already in canonical.
    for (k, v) in extra_headers {
        let kl = k.to_lowercase();
        if !headers.iter().any(|(hk, _)| hk == &kl) {
            wire.push_str(&format!("{}: {}\r\n", k, v));
        }
    }
    wire.push_str(&format!("authorization: {}\r\n", auth_header));
    wire.push_str(&format!("content-length: {}\r\n", body.len()));
    wire.push_str("\r\n");
    wire.push_str(&String::from_utf8_lossy(body));

    let authority_owned = authority.to_string();
    let wire_owned = wire;
    let resp_raw = tokio::task::spawn_blocking(move || -> anyhow::Result<String> {
        let mut stream = TcpStream::connect(authority_owned)?;
        stream.write_all(wire_owned.as_bytes())?;
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf)?;
        Ok(String::from_utf8_lossy(&buf).to_string())
    })
    .await??;

    let mut parts = resp_raw.splitn(2, "\r\n\r\n");
    let head = parts.next().unwrap_or("");
    let body = parts.next().unwrap_or("").to_string();
    let mut lines = head.lines();
    let status_line = lines.next().unwrap_or("");
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let mut hdrs = HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            hdrs.insert(k.trim().to_lowercase(), v.trim().to_string());
        }
    }
    Ok((status, hdrs, body))
}

fn amzdate_from_unix(secs: u64) -> String {
    let s = secs;
    let sec = (s % 60) as u32;
    let m = (s / 60) as u32;
    let min = m % 60;
    let h = (m / 60) as u32;
    let hour = h % 24;
    let mut days = (h / 24) as u64;
    let mut year: u32 = 1970;
    loop {
        let dy = if is_leap(year) { 366 } else { 365 };
        if days >= dy {
            days -= dy;
            year += 1;
        } else {
            break;
        }
    }
    let month_days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let mut month = 0usize;
    while month < 12 {
        let dm = if month == 1 && is_leap(year) { 29 } else { month_days[month] };
        if days >= dm as u64 {
            days -= dm as u64;
            month += 1;
        } else {
            break;
        }
    }
    let day = days as u32 + 1;
    format!("{:04}{:02}{:02}T{:02}{:02}{:02}Z", year, month as u32 + 1, day, hour, min, sec)
}

fn is_leap(y: u32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC");
    mac.update(msg);
    let result = mac.finalize().into_bytes();
    let mut out = [0u8; 32];
    out.copy_from_slice(&result);
    out
}
