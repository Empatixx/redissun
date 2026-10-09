mod common;

use common::{client, raw_command, unique};
use redissun::{BytesCodec, Error, StreamId, StringCodec};

#[tokio::test]
async fn string_codec_stores_stream_fields_and_json_text_as_they_are() {
    let client = client().await;
    let name = unique("stream");
    let events = client
        .with_codec(StringCodec)
        .stream::<String, String>(name.clone());
    let payload = r#"{"id":42,"kind":"order"}"#;
    let id = events.add([("payload", payload)]).await.unwrap();

    let reply = raw_command(&["XRANGE", &name, "-", "+"]).await;
    assert!(reply.contains("\r\npayload\r\n"), "{reply}");
    assert!(reply.contains(&format!("\r\n{payload}\r\n")), "{reply}");

    let entries = events
        .range(&StreamId::min(), &StreamId::max(), None)
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].id, id);
    assert_eq!(
        entries[0].fields,
        vec![("payload".to_string(), payload.to_string())]
    );
}

#[tokio::test]
async fn string_codec_is_readable_by_other_tools() {
    let client = client().await.with_codec(StringCodec);
    let name = unique("bucket");
    client
        .bucket::<String>(name.clone())
        .set("hello")
        .await
        .unwrap();
    assert_eq!(raw_command(&["GET", &name]).await, "$5\r\nhello\r\n");

    let counter = unique("counter");
    raw_command(&["SET", &counter, "41"]).await;
    raw_command(&["INCR", &counter]).await;
    assert_eq!(client.bucket::<i64>(counter).get().await.unwrap(), Some(42));
}

#[tokio::test]
async fn string_codec_works_for_maps() {
    let client = client().await.with_codec(StringCodec);
    let name = unique("map");
    let map = client.hash_map::<String, i64>(name.clone());
    map.insert("visits", &7).await.unwrap();
    assert_eq!(raw_command(&["HGET", &name, "visits"]).await, "$1\r\n7\r\n");
    assert_eq!(map.incr_by("visits", 3).await.unwrap(), 10);
    assert_eq!(map.get("visits").await.unwrap(), Some(10));
}

#[tokio::test]
async fn bytes_codec_keeps_binary_values() {
    let client = client().await.with_codec(BytesCodec);
    let bucket = client.bucket::<Vec<u8>>(unique("bytes"));
    let raw = vec![0u8, 255, 10, 13, 128];
    bucket.set(&raw).await.unwrap();
    assert_eq!(bucket.get().await.unwrap(), Some(raw));
}

#[tokio::test]
async fn the_original_client_keeps_its_codec() {
    let json = client().await;
    let text = json.with_codec(StringCodec);
    let name = unique("bucket");
    text.bucket::<String>(name.clone())
        .set("plain")
        .await
        .unwrap();
    let read = json.bucket::<String>(name).get().await;
    assert!(matches!(read, Err(Error::Codec(_))));
}
