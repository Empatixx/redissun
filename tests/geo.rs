mod common;

use common::{client, unique};
use redissun::{Error, Geo, GeoPoint, GeoUnit, JsonCodec, Object};

const PRAGUE: GeoPoint = GeoPoint {
    longitude: 14.4378,
    latitude: 50.0755,
};
const BRNO: GeoPoint = GeoPoint {
    longitude: 16.6068,
    latitude: 49.1951,
};
const OSTRAVA: GeoPoint = GeoPoint {
    longitude: 18.2625,
    latitude: 49.8209,
};

async fn cities() -> Geo<String, JsonCodec> {
    let geo = client().await.geo::<String>(unique("geo"));
    geo.add("prague", PRAGUE).await.unwrap();
    geo.add("brno", BRNO).await.unwrap();
    geo.add("ostrava", OSTRAVA).await.unwrap();
    geo
}

#[tokio::test]
async fn add_reports_new_members_and_moves_old_ones() {
    let geo = client().await.geo::<String>(unique("geo"));
    assert!(geo.add("a", PRAGUE).await.unwrap());
    assert!(!geo.add("a", BRNO).await.unwrap());
    let moved = geo.pos("a").await.unwrap().unwrap();
    assert!((moved.longitude - BRNO.longitude).abs() < 1e-4);
    assert_eq!(geo.len().await.unwrap(), 1);
}

#[tokio::test]
async fn pos_and_remove() {
    let geo = cities().await;
    let prague = geo.pos("prague").await.unwrap().unwrap();
    assert!((prague.latitude - PRAGUE.latitude).abs() < 1e-4);
    assert_eq!(geo.pos("nowhere").await.unwrap(), None);
    assert!(geo.remove("prague").await.unwrap());
    assert!(!geo.remove("prague").await.unwrap());
    assert_eq!(geo.pos("prague").await.unwrap(), None);
}

#[tokio::test]
async fn dist_uses_the_unit() {
    let geo = cities().await;
    let km = geo
        .dist("prague", "brno", GeoUnit::Kilometers)
        .await
        .unwrap()
        .unwrap();
    assert!((km - 184.0).abs() < 3.0, "{km}");
    let m = geo
        .dist("prague", "brno", GeoUnit::Meters)
        .await
        .unwrap()
        .unwrap();
    assert!((m - km * 1000.0).abs() < 50.0);
    assert_eq!(
        geo.dist("prague", "nowhere", GeoUnit::Meters)
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn hash_is_a_geohash_string() {
    let geo = cities().await;
    let hash = geo.hash("prague").await.unwrap().unwrap();
    assert_eq!(hash.len(), 11);
    assert_eq!(geo.hash("nowhere").await.unwrap(), None);
}

#[tokio::test]
async fn radius_lists_members_nearest_first_with_distance() {
    let geo = cities().await;
    let found = geo
        .radius(BRNO, 300.0, GeoUnit::Kilometers, None)
        .await
        .unwrap();
    let names: Vec<&str> = found.iter().map(|m| m.value.as_str()).collect();
    assert_eq!(names, ["brno", "ostrava", "prague"]);
    assert!(found[0].distance < 1.0);
    assert!(found[1].distance < found[2].distance);
    assert!((found[0].point.latitude - BRNO.latitude).abs() < 1e-4);

    let close = geo
        .radius(BRNO, 100.0, GeoUnit::Kilometers, None)
        .await
        .unwrap();
    assert_eq!(close.len(), 1);
    let limited = geo
        .radius(BRNO, 300.0, GeoUnit::Kilometers, Some(2))
        .await
        .unwrap();
    assert_eq!(limited.len(), 2);
}

#[tokio::test]
async fn radius_of_a_member_includes_the_member() {
    let geo = cities().await;
    let found = geo
        .radius_of("prague", 200.0, GeoUnit::Kilometers, None)
        .await
        .unwrap();
    let names: Vec<&str> = found.iter().map(|m| m.value.as_str()).collect();
    assert_eq!(names, ["prague", "brno"]);
    assert!(geo
        .radius_of("nowhere", 200.0, GeoUnit::Kilometers, None)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn within_box_uses_width_and_height() {
    let geo = cities().await;
    let found = geo
        .within_box(BRNO, 100.0, 100.0, GeoUnit::Kilometers, None)
        .await
        .unwrap();
    assert_eq!(found.len(), 1);
    let wide = geo
        .within_box(BRNO, 800.0, 400.0, GeoUnit::Kilometers, None)
        .await
        .unwrap();
    assert_eq!(wide.len(), 3);
}

#[tokio::test]
async fn invalid_coordinates_are_config_errors() {
    let geo = client().await.geo::<String>(unique("geo"));
    let bad = GeoPoint {
        longitude: 181.0,
        latitude: 0.0,
    };
    assert!(matches!(geo.add("x", bad).await, Err(Error::Config(_))));
    let pole = GeoPoint {
        longitude: 0.0,
        latitude: 89.0,
    };
    assert!(matches!(geo.add("x", pole).await, Err(Error::Config(_))));
    assert!(matches!(
        geo.radius(bad, 1.0, GeoUnit::Meters, None).await,
        Err(Error::Config(_))
    ));
    let nan = GeoPoint {
        longitude: f64::NAN,
        latitude: 0.0,
    };
    assert!(matches!(geo.add("x", nan).await, Err(Error::Config(_))));
}

#[tokio::test]
async fn a_negative_radius_is_a_config_error() {
    let geo = cities().await;
    assert!(matches!(
        geo.radius(BRNO, -1.0, GeoUnit::Meters, None).await,
        Err(Error::Config(_))
    ));
}

#[tokio::test]
async fn clear_and_object_methods() {
    let geo = cities().await;
    assert!(geo.exists().await.unwrap());
    geo.clear().await.unwrap();
    assert!(geo.is_empty().await.unwrap());
    geo.add("again", PRAGUE).await.unwrap();
    assert!(geo.del().await.unwrap());
}

#[tokio::test]
async fn a_count_of_zero_returns_nothing() {
    let geo = cities().await;
    assert!(geo
        .radius(BRNO, 300.0, GeoUnit::Kilometers, Some(0))
        .await
        .unwrap()
        .is_empty());
}
