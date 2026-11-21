use crate::common::{client, connect_with, unique};
use redissun::Adder;
use std::time::Duration;

async fn three<N: redissun::Summable>() -> [Adder<N>; 3] {
    let client = client().await;
    let name = unique("adder");
    [
        client.adder(name.clone()).await.unwrap(),
        client.adder(name.clone()).await.unwrap(),
        client.adder(name).await.unwrap(),
    ]
}

async fn sum_once() {
    let adders = three::<i64>().await;
    adders[0].add(2);
    adders[1].add(4);
    adders[2].add(1);

    for adder in &adders {
        assert_eq!(adder.sum().await.unwrap(), 7);
    }
}

async fn reset_once() {
    let adders = three::<i64>().await;
    adders[0].add(2);
    adders[1].add(4);
    adders[2].add(1);

    adders[0].reset().await.unwrap();

    for adder in &adders {
        assert_eq!(adder.sum().await.unwrap(), 0);
    }
}

#[tokio::test]
async fn sum() {
    sum_once().await;
}

#[tokio::test]
async fn reset() {
    reset_once().await;
}

#[tokio::test]
async fn sum_stability() {
    for _ in 0..100 {
        sum_once().await;
    }
}

#[tokio::test]
async fn reset_stability() {
    for _ in 0..100 {
        reset_once().await;
    }
}

#[tokio::test]
async fn f64_sum() {
    let adders = three::<f64>().await;
    adders[0].add(2.38);
    adders[1].add(4.14);
    adders[2].add(1.48);

    for adder in &adders {
        assert_eq!(adder.sum().await.unwrap(), 8.0);
    }
}

#[tokio::test]
async fn f64_reset() {
    let adders = three::<f64>().await;
    adders[0].add(2.0);
    adders[1].add(4.0);
    adders[2].add(1.0);

    adders[0].reset().await.unwrap();

    for adder in &adders {
        assert_eq!(adder.sum().await.unwrap(), 0.0);
    }
}

#[tokio::test]
async fn the_sum_covers_every_client() {
    let name = unique("adder");
    let first = client().await.adder::<i64>(name.clone()).await.unwrap();
    let other_client = connect_with(|builder| builder).await;
    let second = other_client.adder::<i64>(name.clone()).await.unwrap();

    first.incr();
    first.incr();
    second.add(40);
    second.decr();

    let limit = Duration::from_secs(10);
    assert_eq!(first.sum().timeout(limit).await.unwrap(), Some(41));
    assert_eq!(second.sum().timeout(limit).await.unwrap(), Some(41));

    drop(second);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(first.sum().timeout(limit).await.unwrap(), Some(2));

    first.reset().timeout(limit).await.unwrap();
    assert_eq!(first.sum().timeout(limit).await.unwrap(), Some(0));
}

#[tokio::test]
async fn one_name_cannot_count_two_number_types() {
    let client = client().await;
    let name = unique("adder");
    let _whole = client.adder::<i64>(name.clone()).await.unwrap();
    assert!(client.adder::<f64>(name).await.is_err());
}

#[tokio::test]
async fn sum_leaves_no_keys_behind() {
    let client = client().await;
    let name = unique("adder");
    let adder = client.adder::<i64>(name.clone()).await.unwrap();
    adder.add(5);
    assert_eq!(adder.sum().await.unwrap(), 5);
    adder.reset().await.unwrap();

    let left: i64 = client
        .with_codec(redissun::StringCodec)
        .script()
        .eval("return #redis.call('KEYS', ARGV[1])")
        .arg(&format!("*{name}*"))
        .await
        .unwrap();
    assert_eq!(left, 0);
}
