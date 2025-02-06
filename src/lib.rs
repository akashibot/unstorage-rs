use isahc::{AsyncReadResponseExt, Request, RequestExt};
use anyhow::Result;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use chrono::{DateTime, Utc};
use isahc::http::HeaderName;
use serde::{Serialize, de::DeserializeOwned};

pub struct UnstorageClient {
    base_url: String,
    headers: HashMap<String, String>,
}

impl UnstorageClient {
    /// Create a new Unstorage client with the given base URL and custom headers.
    pub fn new(base_url: String, headers: Option<HashMap<String, String>>) -> Self {
        Self {
            base_url,
            headers: headers.unwrap_or_default(),
        }
    }

    /// Get headers for a request, including transaction options.
    fn get_headers(&self, topts: Option<&TransactionOptions>) -> HashMap<String, String> {
        let mut headers = self.headers.clone();
        if let Some(topts) = topts {
            if let Some(ttl) = topts.ttl {
                headers.insert("x-ttl".to_string(), ttl.to_string());
            }
            if let Some(extra_headers) = &topts.headers {
                headers.extend(extra_headers.clone());
            }
        }
        headers
    }

    /// Helper function to add headers to a request.
    fn add_headers_to_request<R>(&self, request: &mut Request<R>, topts: Option<&TransactionOptions>) -> Result<()> {
        for (key, value) in self.get_headers(topts) {
            let header_name = HeaderName::from_bytes(key.as_bytes())?;
            request.headers_mut().insert(header_name, value.parse()?);
        }
        Ok(())
    }

    /// Check if an item exists in the storage by key.
    pub async fn has_item(&self, key: &str, topts: Option<&TransactionOptions>) -> Result<bool> {
        let url = format!("{}/{}", self.base_url, key);
        let mut request = Request::head(url).body(())?;
        self.add_headers_to_request(&mut request, topts)?;

        let response = request.send_async().await?;
        Ok(response.status().is_success())
    }

    /// Get an item from the storage by key and return it as a string.
    pub async fn get_item(&self, key: &str, topts: Option<&TransactionOptions>) -> Result<Option<String>> {
        let url = format!("{}/{}", self.base_url, key);
        let mut request = Request::get(url).body(())?;
        self.add_headers_to_request(&mut request, topts)?;

        let mut response = request.send_async().await?;
        if response.status().is_success() {
            let body = response.text().await?;
            Ok(Some(body))
        } else {
            Ok(None)
        }
    }

    /// Get an item from the storage by key and deserialize it into the specified type.
    pub async fn get_item_json<T: DeserializeOwned>(
        &self,
        key: &str,
        topts: Option<&TransactionOptions>,
    ) -> Result<Option<T>> {
        let url = format!("{}/{}", self.base_url, key);
        let mut request = Request::get(url).body(())?;
        self.add_headers_to_request(&mut request, topts)?;

        let mut response = request.send_async().await?;
        if response.status().is_success() {
            let body = response.text().await?;
            let deserialized: T = serde_json::from_str(&body)?;
            Ok(Some(deserialized))
        } else {
            Ok(None)
        }
    }

    /// Get an item in binary mode.
    pub async fn get_item_raw(&self, key: &str, topts: Option<&TransactionOptions>) -> Result<Option<Vec<u8>>> {
        let url = format!("{}/{}", self.base_url, key);
        let mut request = Request::get(url).body(())?;

        let mut headers = self.get_headers(topts);
        headers.insert("accept".to_string(), "application/octet-stream".to_string());

        for (key, value) in headers {
            let header_name = HeaderName::from_bytes(key.as_bytes())?;
            request.headers_mut().insert(header_name, value.parse()?);
        }

        let mut response = request.send_async().await?;
        if response.status().is_success() {
            let body = response.bytes().await?;
            Ok(Some(body.to_vec()))
        } else {
            Ok(None)
        }
    }

    /// Get metadata for an item (mtime and ttl from headers).
    pub async fn get_meta(&self, key: &str, topts: Option<&TransactionOptions>) -> Result<Option<Meta>> {
        let url = format!("{}/{}", self.base_url, key);
        let mut request = Request::head(url).body(())?;
        self.add_headers_to_request(&mut request, topts)?;

        let response = request.send_async().await?;
        if response.status().is_success() {
            let headers = response.headers();
            let mtime = headers
                .get("last-modified")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| DateTime::parse_from_rfc2822(s).ok())
                .map(|dt| dt.with_timezone(&Utc).into());

            let ttl = headers
                .get("x-ttl")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .map(Duration::from_secs);

            Ok(Some(Meta { mtime, ttl }))
        } else {
            Ok(None)
        }
    }

    /// Set an item in the storage with the given key, value, and optional TTL.
    pub async fn set_item(&self, key: &str, value: &str, topts: Option<&TransactionOptions>) -> Result<()> {
        let url = format!("{}/{}", self.base_url, key);
        let mut request = Request::put(url)
            .header("Content-Type", "application/json")
            .body(value.to_string())?;
        self.add_headers_to_request(&mut request, topts)?;

        let _response = request.send_async().await?;
        Ok(())
    }

    /// Set an item in binary mode.
    pub async fn set_item_raw(&self, key: &str, value: &[u8], topts: Option<&TransactionOptions>) -> Result<()> {
        let url = format!("{}/{}", self.base_url, key);
        let mut request = Request::put(url)
            .header("Content-Type", "application/octet-stream")
            .body(value.to_vec())?;
        self.add_headers_to_request(&mut request, topts)?;

        let _response = request.send_async().await?;
        Ok(())
    }

    /// Set an item in JSON format.
    pub async fn set_item_json<T: Serialize>(
        &self,
        key: &str,
        value: &T,
        topts: Option<&TransactionOptions>,
    ) -> Result<()> {
        let url = format!("{}/{}", self.base_url, key);
        let json_body = serde_json::to_string(value)?;
        let mut request = Request::put(url)
            .header("Content-Type", "application/json")
            .body(json_body)?;
        self.add_headers_to_request(&mut request, topts)?;

        let _response = request.send_async().await?;
        Ok(())
    }

    /// Remove an item from the storage by key.
    pub async fn remove_item(&self, key: &str, topts: Option<&TransactionOptions>) -> Result<()> {
        let url = format!("{}/{}", self.base_url, key);
        let mut request = Request::delete(url).body(())?;
        self.add_headers_to_request(&mut request, topts)?;

        let _response = request.send_async().await?;
        Ok(())
    }

    /// Get keys from the storage (when the path ends with `/` or `/:`).
    pub async fn get_keys(&self, base: &str, topts: Option<&TransactionOptions>) -> Result<Option<Vec<String>>> {
        let url = format!("{}/{}:", self.base_url, base);
        let mut request = Request::get(url).body(())?;
        self.add_headers_to_request(&mut request, topts)?;

        let mut response = request.send_async().await?;
        if response.status().is_success() {
            let body = response.text().await?;
            let keys: Vec<String> = serde_json::from_str(&body)?;
            Ok(Some(keys))
        } else {
            Ok(None)
        }
    }

    /// Clear all items in the storage (when the path ends with `/` or `/:`).
    pub async fn clear(&self, base: &str, topts: Option<&TransactionOptions>) -> Result<()> {
        let url = format!("{}/{}:", self.base_url, base);
        let mut request = Request::delete(url).body(())?;
        self.add_headers_to_request(&mut request, topts)?;

        let _response = request.send_async().await?;
        Ok(())
    }
}

/// Metadata for an item (mtime and ttl).
#[derive(Debug)]
pub struct Meta {
    pub mtime: Option<SystemTime>,
    pub ttl: Option<Duration>,
}

/// Transaction options for storage operations.
#[derive(Debug)]
pub struct TransactionOptions {
    pub headers: Option<HashMap<String, String>>,
    pub ttl: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Serialize, Deserialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct TestData {
        name: String,
        age: u32,
    }

    #[tokio::test]
    async fn test_unstorage_client() {
        let client = UnstorageClient::new("http://localhost:3000".to_string(), None);
        let key = "test";
        let value = "Hello, World!";

        client.set_item(key, value, None).await.unwrap();
        let item = client.get_item(key, None).await.unwrap().unwrap();
        assert_eq!(item, value);
    }

    #[tokio::test]
    async fn test_set_and_get_item_json() {
        let client = UnstorageClient::new("http://localhost:3000".to_string(), None);
        let key = "test_json";
        let value = TestData {
            name: "Alice".to_string(),
            age: 30,
        };

        // Set JSON data
        client.set_item_json(key, &value, None).await.unwrap();

        // Get JSON data and deserialize it
        let retrieved = client.get_item_json::<TestData>(key, None).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), value);
    }
}