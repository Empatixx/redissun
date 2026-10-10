use crate::common::{client, unique};
use redissun::{Error, Geo, GeoMatch, GeoOrder, GeoPoint, GeoSearch, GeoUnit, JsonCodec, Object};
use std::collections::BTreeMap;

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

fn at(longitude: f64, latitude: f64) -> GeoPoint {
    GeoPoint {
        longitude,
        latitude,
    }
}

const PALERMO_POS: GeoPoint = GeoPoint {
    longitude: 13.361389338970184,
    latitude: 38.1155563954963,
};
const CATANIA_POS: GeoPoint = GeoPoint {
    longitude: 15.087267458438873,
    latitude: 37.50266842333162,
};

fn close(a: GeoPoint, b: GeoPoint) -> bool {
    (a.longitude - b.longitude).abs() < 1e-9 && (a.latitude - b.latitude).abs() < 1e-9
}

async fn named(name: &str) -> Geo<String, JsonCodec> {
    client().await.geo::<String>(name.to_string())
}

async fn sicily_at(name: &str) -> Geo<String, JsonCodec> {
    let geo = named(name).await;
    geo.extend([
        ("Palermo", at(13.361389, 38.115556)),
        ("Catania", at(15.087269, 37.502669)),
    ])
    .await
    .unwrap();
    geo
}

async fn sicily() -> Geo<String, JsonCodec> {
    sicily_at(&unique("test")).await
}

fn names<V: Clone>(found: &[GeoMatch<V>]) -> Vec<V> {
    found.iter().map(|m| m.value.clone()).collect()
}

fn distances(found: &[GeoMatch<String>]) -> Vec<(String, f64)> {
    found
        .iter()
        .map(|m| (m.value.clone(), m.distance))
        .collect()
}

fn from_15_37() -> GeoPoint {
    at(15.0, 37.0)
}

fn km200() -> GeoSearch {
    GeoSearch::radius(200.0, GeoUnit::Kilometers)
}

fn store_pair() -> (String, String) {
    let tag = unique("geo");
    (format!("{{{tag}}}:test"), format!("{{{tag}}}:test-store"))
}

#[tokio::test]
async fn test_add() {
    let geo = client().await.geo::<String>(unique("test"));
    assert!(geo.add("city1", at(2.51, 3.12)).await.unwrap());
}

#[tokio::test]
async fn test_add_if_exists() {
    let geo = client().await.geo::<String>(unique("test"));
    assert!(geo.add("city1", at(2.51, 3.12)).await.unwrap());
    assert!(geo.add_if_exists("city1", at(2.9, 3.9)).await.unwrap());
    let pos = geo.pos("city1").await.unwrap().unwrap();
    assert!((3.8..=3.9).contains(&pos.latitude));
    assert!((2.8..=3.0).contains(&pos.longitude));
    assert!(!geo.add_if_exists("city2", at(2.12, 3.5)).await.unwrap());
}

#[tokio::test]
async fn test_try_add() {
    let geo = client().await.geo::<String>(unique("test"));
    assert!(geo.add("city1", at(2.51, 3.12)).await.unwrap());
    assert!(!geo.try_add("city1", at(2.5, 3.1)).await.unwrap());
    assert!(geo.try_add("city2", at(2.12, 3.5)).await.unwrap());
}

#[tokio::test]
async fn test_add_entries() {
    let geo = client().await.geo::<String>(unique("test"));
    let added = geo
        .extend([
            ("city1", at(3.11, 9.10321)),
            ("city2", at(81.1231, 38.65478)),
        ])
        .await
        .unwrap();
    assert_eq!(added, 2);
}

#[tokio::test]
async fn test_dist() {
    let geo = sicily().await;
    assert_eq!(
        geo.dist("Palermo", "Catania", GeoUnit::Meters)
            .await
            .unwrap(),
        Some(166274.1516)
    );
}

#[tokio::test]
async fn test_dist_empty() {
    let geo = client().await.geo::<String>(unique("test"));
    assert_eq!(
        geo.dist("Palermo", "Catania", GeoUnit::Meters)
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn test_hash() {
    let geo = sicily().await;
    assert_eq!(
        geo.hash_many(&["Palermo", "Catania"]).await.unwrap(),
        [
            Some("sqc8b49rny0".to_string()),
            Some("sqdtr74hyu0".to_string())
        ]
    );
}

#[tokio::test]
async fn test_hash_empty() {
    let geo = client().await.geo::<String>(unique("test"));
    assert_eq!(
        geo.hash_many(&["Palermo", "Catania"]).await.unwrap(),
        [None, None]
    );
}

#[tokio::test]
async fn test_pos4() {
    let geo = sicily().await;
    let pos = geo.pos_many(&["Palermo", "Catania"]).await.unwrap();
    assert!(close(pos[0].unwrap(), PALERMO_POS));
    assert!(close(pos[1].unwrap(), CATANIA_POS));
}

#[tokio::test]
async fn test_pos1() {
    let geo = client().await.geo::<String>(unique("test"));
    geo.add("hi", at(0.123, 0.893)).await.unwrap();
    assert!(geo.pos("hi").await.unwrap().is_some());
}

#[tokio::test]
async fn test_pos3() {
    let geo = client().await.geo::<String>(unique("test"));
    geo.add("hi", at(0.123, 0.893)).await.unwrap();
    let pos = geo.pos_many(&["hi", "123f", "sdfdsf"]).await.unwrap();
    assert!(pos[0].is_some());
    assert_eq!(pos[1..], [None, None]);
}

#[tokio::test]
async fn test_pos2() {
    let geo = client().await.geo::<String>(unique("test"));
    geo.add("Palermo", at(13.361389, 38.115556)).await.unwrap();
    let pos = geo
        .pos_many(&["test2", "Palermo", "test3", "Catania", "test1"])
        .await
        .unwrap();
    assert!(close(pos[1].unwrap(), PALERMO_POS));
    assert_eq!(pos.iter().filter(|p| p.is_some()).count(), 1);
}

#[tokio::test]
async fn test_pos() {
    let geo = sicily().await;
    let pos = geo
        .pos_many(&["test2", "Palermo", "test3", "Catania", "test1"])
        .await
        .unwrap();
    assert!(close(pos[1].unwrap(), PALERMO_POS));
    assert!(close(pos[3].unwrap(), CATANIA_POS));
    assert_eq!(pos.iter().filter(|p| p.is_some()).count(), 2);
}

#[tokio::test]
async fn test_pos_empty() {
    let geo = client().await.geo::<String>(unique("test"));
    let pos = geo
        .pos_many(&["test2", "Palermo", "test3", "Catania", "test1"])
        .await
        .unwrap();
    assert!(pos.iter().all(Option::is_none));
}

#[tokio::test]
async fn test_box() {
    let geo = sicily().await;
    let found = geo
        .search(
            at(15.5, 38.5),
            &GeoSearch::rect(5400.0, 5400.0, GeoUnit::Kilometers),
        )
        .await
        .unwrap();
    assert_eq!(names(&found), ["Palermo", "Catania"]);
}

#[tokio::test]
async fn test_box_with_distance() {
    let geo = sicily().await;
    let found = geo
        .search(
            at(15.5, 38.5),
            &GeoSearch::rect(5400.0, 5400.0, GeoUnit::Kilometers),
        )
        .await
        .unwrap();
    let found: BTreeMap<String, f64> = distances(&found).into_iter().collect();
    assert_eq!(
        found,
        BTreeMap::from([
            ("Palermo".to_string(), 191.4848),
            ("Catania".to_string(), 116.6784)
        ])
    );
}

#[tokio::test]
async fn test_box_with_position() {
    let geo = sicily().await;
    let found = geo
        .search(
            at(15.5, 38.5),
            &GeoSearch::rect(5400.0, 5400.0, GeoUnit::Kilometers),
        )
        .await
        .unwrap();
    assert_eq!(found.len(), 2);
    assert!(close(found[0].point, PALERMO_POS));
    assert!(close(found[1].point, CATANIA_POS));
}

#[tokio::test]
async fn test_box_store_search() {
    let (source, dest) = store_pair();
    let geo_source = sicily_at(&source).await;
    let stored = geo_source
        .store_search_to(
            &dest,
            at(15.5, 38.5),
            &GeoSearch::rect(5400.0, 5400.0, GeoUnit::Kilometers),
        )
        .await
        .unwrap();
    assert_eq!(stored, 2);
    let mut all = named(&dest).await.read_all().await.unwrap();
    all.sort();
    assert_eq!(all, ["Catania", "Palermo"]);
}

#[tokio::test]
async fn test_box_store_sorted() {
    let (source, dest) = store_pair();
    let geo_source = sicily_at(&source).await;
    let stored = geo_source
        .store_search_to(
            &dest,
            at(15.0, 37.0),
            &GeoSearch::rect(5400.0, 5400.0, GeoUnit::Kilometers).store_dist(),
        )
        .await
        .unwrap();
    assert_eq!(stored, 2);
    assert_eq!(
        named(&dest).await.read_all().await.unwrap(),
        ["Catania", "Palermo"]
    );
}

#[tokio::test]
async fn test_radius() {
    let geo = sicily().await;
    let found = geo.search(from_15_37(), &km200()).await.unwrap();
    assert_eq!(names(&found), ["Palermo", "Catania"]);
}

#[tokio::test]
async fn test_radius_count() {
    let geo = sicily().await;
    let found = geo.search(from_15_37(), &km200().count(1)).await.unwrap();
    assert_eq!(names(&found), ["Catania"]);
}

#[tokio::test]
async fn test_radius_order() {
    let geo = sicily().await;
    let desc = geo
        .search(from_15_37(), &km200().order(GeoOrder::Desc))
        .await
        .unwrap();
    assert_eq!(names(&desc), ["Palermo", "Catania"]);
    let asc = geo
        .search(from_15_37(), &km200().order(GeoOrder::Asc))
        .await
        .unwrap();
    assert_eq!(names(&asc), ["Catania", "Palermo"]);
}

#[tokio::test]
async fn test_radius_order_count() {
    let geo = sicily().await;
    let desc = geo
        .search(from_15_37(), &km200().order(GeoOrder::Desc).count(1))
        .await
        .unwrap();
    assert_eq!(names(&desc), ["Palermo"]);
    let asc = geo
        .search(from_15_37(), &km200().order(GeoOrder::Asc).count(1))
        .await
        .unwrap();
    assert_eq!(names(&asc), ["Catania"]);
}

#[tokio::test]
async fn test_radius_empty() {
    let geo = client().await.geo::<String>(unique("test"));
    assert!(geo.search(from_15_37(), &km200()).await.unwrap().is_empty());
}

#[tokio::test]
async fn test_radius_with_distance() {
    let geo = sicily().await;
    let found = geo.search(from_15_37(), &km200()).await.unwrap();
    let found: BTreeMap<String, f64> = distances(&found).into_iter().collect();
    assert_eq!(
        found,
        BTreeMap::from([
            ("Palermo".to_string(), 190.4424),
            ("Catania".to_string(), 56.4413)
        ])
    );
}

#[tokio::test]
async fn test_radius_with_distance_count() {
    let geo = sicily().await;
    let found = geo.search(from_15_37(), &km200().count(1)).await.unwrap();
    assert_eq!(distances(&found), [("Catania".to_string(), 56.4413)]);
}

#[tokio::test]
async fn test_radius_with_distance_order() {
    let geo = sicily().await;
    let desc = geo
        .search(from_15_37(), &km200().order(GeoOrder::Desc))
        .await
        .unwrap();
    assert_eq!(
        distances(&desc),
        [
            ("Palermo".to_string(), 190.4424),
            ("Catania".to_string(), 56.4413)
        ]
    );
    let asc = geo
        .search(from_15_37(), &km200().order(GeoOrder::Asc))
        .await
        .unwrap();
    assert_eq!(
        distances(&asc),
        [
            ("Catania".to_string(), 56.4413),
            ("Palermo".to_string(), 190.4424)
        ]
    );
}

#[tokio::test]
async fn test_radius_with_distance_order_count() {
    let geo = sicily().await;
    let desc = geo
        .search(from_15_37(), &km200().order(GeoOrder::Desc).count(1))
        .await
        .unwrap();
    assert_eq!(distances(&desc), [("Palermo".to_string(), 190.4424)]);
    let asc = geo
        .search(from_15_37(), &km200().order(GeoOrder::Asc).count(1))
        .await
        .unwrap();
    assert_eq!(distances(&asc), [("Catania".to_string(), 56.4413)]);
}

async fn huge() -> Geo<String, JsonCodec> {
    let geo = client().await.geo::<String>(unique("test"));
    let members: Vec<String> = (0..10000).map(|i| i.to_string()).collect();
    geo.extend(members.iter().enumerate().map(|(i, member)| {
        (
            member.as_str(),
            at(10.0 + 0.000001 * i as f64, 11.0 + 0.000001 * i as f64),
        )
    }))
    .await
    .unwrap();
    geo
}

#[tokio::test]
async fn test_radius_with_distance_huge_amount() {
    let geo = huge().await;
    let found = geo.search(at(10.0, 11.0), &km200()).await.unwrap();
    assert_eq!(found.len(), 10000);
}

#[tokio::test]
async fn test_radius_with_position_huge_amount() {
    let geo = huge().await;
    let found = geo.search(at(10.0, 11.0), &km200()).await.unwrap();
    assert_eq!(found.len(), 10000);
    assert!(found.iter().all(|m| (m.point.longitude - 10.0).abs() < 0.1));
}

#[tokio::test]
async fn test_radius_with_distance_big_object() {
    let geo = client()
        .await
        .geo::<BTreeMap<String, String>>(unique("test"));
    let map: BTreeMap<String, String> = (0..150).map(|i| (i.to_string(), i.to_string())).collect();
    geo.add(&map, at(13.361389, 38.115556)).await.unwrap();
    let mut map1 = map.clone();
    map1.remove("100");
    geo.add(&map1, at(15.087269, 37.502669)).await.unwrap();
    let mut map2 = map.clone();
    map2.remove("0");
    geo.add(&map2, at(15.081269, 37.502169)).await.unwrap();

    let found = geo.search(from_15_37(), &km200()).await.unwrap();
    let distance = |wanted: &BTreeMap<String, String>| {
        found
            .iter()
            .find(|m| &m.value == wanted)
            .map(|m| m.distance)
    };
    assert_eq!(found.len(), 3);
    assert_eq!(distance(&map), Some(190.4424));
    assert_eq!(distance(&map1), Some(56.4413));
    assert_eq!(distance(&map2), Some(56.3159));
}

#[tokio::test]
async fn test_radius_with_distance_empty() {
    let geo = client().await.geo::<String>(unique("test"));
    assert!(geo.search(from_15_37(), &km200()).await.unwrap().is_empty());
}

#[tokio::test]
async fn test_radius_with_position() {
    let geo = sicily().await;
    let found = geo.search(from_15_37(), &km200()).await.unwrap();
    assert_eq!(names(&found), ["Palermo", "Catania"]);
    assert!(close(found[0].point, PALERMO_POS));
    assert!(close(found[1].point, CATANIA_POS));
}

#[tokio::test]
async fn test_radius_with_position_count() {
    let geo = sicily().await;
    let found = geo.search(from_15_37(), &km200().count(1)).await.unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].value, "Catania");
    assert!(close(found[0].point, CATANIA_POS));
}

#[tokio::test]
async fn test_radius_with_position_order() {
    let geo = sicily().await;
    let desc = geo
        .search(from_15_37(), &km200().order(GeoOrder::Desc))
        .await
        .unwrap();
    assert_eq!(names(&desc), ["Palermo", "Catania"]);
    assert!(close(desc[0].point, PALERMO_POS) && close(desc[1].point, CATANIA_POS));
    let asc = geo
        .search(from_15_37(), &km200().order(GeoOrder::Asc))
        .await
        .unwrap();
    assert_eq!(names(&asc), ["Catania", "Palermo"]);
    assert!(close(asc[0].point, CATANIA_POS) && close(asc[1].point, PALERMO_POS));
}

#[tokio::test]
async fn test_radius_with_position_order_count() {
    let geo = sicily().await;
    let desc = geo
        .search(from_15_37(), &km200().order(GeoOrder::Desc).count(1))
        .await
        .unwrap();
    assert_eq!(names(&desc), ["Palermo"]);
    assert!(close(desc[0].point, PALERMO_POS));
    let asc = geo
        .search(from_15_37(), &km200().order(GeoOrder::Asc).count(1))
        .await
        .unwrap();
    assert_eq!(names(&asc), ["Catania"]);
    assert!(close(asc[0].point, CATANIA_POS));
}

#[tokio::test]
async fn test_radius_with_position_empty() {
    let geo = client().await.geo::<String>(unique("test"));
    assert!(geo.search(from_15_37(), &km200()).await.unwrap().is_empty());
}

#[tokio::test]
async fn test_radius_member() {
    let geo = sicily().await;
    let found = geo.search_from("Palermo", &km200()).await.unwrap();
    assert_eq!(names(&found), ["Palermo", "Catania"]);
}

#[tokio::test]
async fn test_radius_member_count() {
    let geo = sicily().await;
    let found = geo.search_from("Palermo", &km200().count(1)).await.unwrap();
    assert_eq!(names(&found), ["Palermo"]);
}

#[tokio::test]
async fn test_radius_member_order() {
    let geo = sicily().await;
    let desc = geo
        .search_from("Palermo", &km200().order(GeoOrder::Desc))
        .await
        .unwrap();
    assert_eq!(names(&desc), ["Catania", "Palermo"]);
    let asc = geo
        .search_from("Palermo", &km200().order(GeoOrder::Asc))
        .await
        .unwrap();
    assert_eq!(names(&asc), ["Palermo", "Catania"]);
}

#[tokio::test]
async fn test_radius_member_order_count() {
    let geo = sicily().await;
    let desc = geo
        .search_from("Palermo", &km200().order(GeoOrder::Desc).count(1))
        .await
        .unwrap();
    assert_eq!(names(&desc), ["Catania"]);
    let asc = geo
        .search_from("Palermo", &km200().order(GeoOrder::Asc).count(1))
        .await
        .unwrap();
    assert_eq!(names(&asc), ["Palermo"]);
}

#[tokio::test]
async fn test_radius_member_empty() {
    let geo = client().await.geo::<String>(unique("test"));
    assert!(geo
        .search_from("Palermo", &km200())
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn test_radius_member_with_distance() {
    let geo = sicily().await;
    let found = geo.search_from("Palermo", &km200()).await.unwrap();
    let found: BTreeMap<String, f64> = distances(&found).into_iter().collect();
    assert_eq!(
        found,
        BTreeMap::from([
            ("Palermo".to_string(), 0.0),
            ("Catania".to_string(), 166.2742)
        ])
    );
}

#[tokio::test]
async fn test_radius_member_with_distance_count() {
    let geo = sicily().await;
    let found = geo.search_from("Palermo", &km200().count(1)).await.unwrap();
    assert_eq!(distances(&found), [("Palermo".to_string(), 0.0)]);
}

#[tokio::test]
async fn test_radius_member_with_distance_order() {
    let geo = sicily().await;
    let asc = geo
        .search_from("Palermo", &km200().order(GeoOrder::Asc))
        .await
        .unwrap();
    assert_eq!(
        distances(&asc),
        [
            ("Palermo".to_string(), 0.0),
            ("Catania".to_string(), 166.2742)
        ]
    );
    let desc = geo
        .search_from("Palermo", &km200().order(GeoOrder::Desc))
        .await
        .unwrap();
    assert_eq!(
        distances(&desc),
        [
            ("Catania".to_string(), 166.2742),
            ("Palermo".to_string(), 0.0)
        ]
    );
}

#[tokio::test]
async fn test_radius_member_with_distance_order_count() {
    let geo = sicily().await;
    let asc = geo
        .search_from("Palermo", &km200().order(GeoOrder::Asc).count(1))
        .await
        .unwrap();
    assert_eq!(distances(&asc), [("Palermo".to_string(), 0.0)]);
    let desc = geo
        .search_from("Palermo", &km200().order(GeoOrder::Desc).count(1))
        .await
        .unwrap();
    assert_eq!(distances(&desc), [("Catania".to_string(), 166.2742)]);
}

#[tokio::test]
async fn test_radius_member_with_distance_empty() {
    let geo = client().await.geo::<String>(unique("test"));
    assert!(geo
        .search_from("Palermo", &km200())
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn test_radius_member_with_position() {
    let geo = sicily().await;
    let found = geo.search_from("Palermo", &km200()).await.unwrap();
    assert_eq!(names(&found), ["Palermo", "Catania"]);
    assert!(close(found[0].point, PALERMO_POS) && close(found[1].point, CATANIA_POS));
}

#[tokio::test]
async fn test_radius_member_with_position_count() {
    let geo = sicily().await;
    let found = geo.search_from("Palermo", &km200().count(1)).await.unwrap();
    assert_eq!(names(&found), ["Palermo"]);
    assert!(close(found[0].point, PALERMO_POS));
}

#[tokio::test]
async fn test_radius_member_with_position_order() {
    let geo = sicily().await;
    let asc = geo
        .search_from("Palermo", &km200().order(GeoOrder::Asc))
        .await
        .unwrap();
    assert_eq!(names(&asc), ["Palermo", "Catania"]);
    assert!(close(asc[0].point, PALERMO_POS) && close(asc[1].point, CATANIA_POS));
    let desc = geo
        .search_from("Palermo", &km200().order(GeoOrder::Desc))
        .await
        .unwrap();
    assert_eq!(names(&desc), ["Catania", "Palermo"]);
    assert!(close(desc[0].point, CATANIA_POS) && close(desc[1].point, PALERMO_POS));
}

#[tokio::test]
async fn test_radius_member_with_position_order_count() {
    let geo = sicily().await;
    let asc = geo
        .search_from("Palermo", &km200().order(GeoOrder::Asc).count(1))
        .await
        .unwrap();
    assert_eq!(names(&asc), ["Palermo"]);
    assert!(close(asc[0].point, PALERMO_POS));
    let desc = geo
        .search_from("Palermo", &km200().order(GeoOrder::Desc).count(1))
        .await
        .unwrap();
    assert_eq!(names(&desc), ["Catania"]);
    assert!(close(desc[0].point, CATANIA_POS));

    let geo2 = client().await.geo::<String>(unique("test2"));
    geo2.extend([
        ("Palermo", at(13.361389, 38.115556)),
        ("Catania", at(13.361390, 38.115557)),
    ])
    .await
    .unwrap();
    let both = geo2
        .search_from("Palermo", &km200().order(GeoOrder::Desc).count(2))
        .await
        .unwrap();
    let mut both = names(&both);
    both.sort();
    assert_eq!(both, ["Catania", "Palermo"]);
}

#[tokio::test]
async fn test_radius_member_with_position_empty() {
    let geo = client().await.geo::<String>(unique("test"));
    assert!(geo
        .search_from("Palermo", &km200())
        .await
        .unwrap()
        .is_empty());
}

async fn stored(dest: &str) -> Vec<String> {
    named(dest).await.read_all().await.unwrap()
}

#[tokio::test]
async fn test_radius_store() {
    let (source, dest) = store_pair();
    let geo_source = sicily_at(&source).await;
    assert_eq!(
        geo_source
            .store_search_to(&dest, from_15_37(), &km200())
            .await
            .unwrap(),
        2
    );
    let mut all = stored(&dest).await;
    all.sort();
    assert_eq!(all, ["Catania", "Palermo"]);
}

#[tokio::test]
async fn test_radius_store_sorted() {
    let (source, dest) = store_pair();
    let geo_source = sicily_at(&source).await;
    assert_eq!(
        geo_source
            .store_search_to(&dest, from_15_37(), &km200().store_dist())
            .await
            .unwrap(),
        2
    );
    assert_eq!(stored(&dest).await, ["Catania", "Palermo"]);
}

#[tokio::test]
async fn test_radius_store_count() {
    let (source, dest) = store_pair();
    let geo_source = sicily_at(&source).await;
    assert_eq!(
        geo_source
            .store_search_to(&dest, from_15_37(), &km200().count(1))
            .await
            .unwrap(),
        1
    );
    assert_eq!(stored(&dest).await, ["Catania"]);
}

#[tokio::test]
async fn test_radius_store_sorted_count() {
    let (source, dest) = store_pair();
    let geo_source = sicily_at(&source).await;
    assert_eq!(
        geo_source
            .store_search_to(&dest, from_15_37(), &km200().count(1).store_dist())
            .await
            .unwrap(),
        1
    );
    assert_eq!(stored(&dest).await, ["Catania"]);
}

#[tokio::test]
async fn test_radius_store_order_count() {
    let (source, dest) = store_pair();
    let geo_source = sicily_at(&source).await;
    assert_eq!(
        geo_source
            .store_search_to(&dest, from_15_37(), &km200().order(GeoOrder::Desc).count(1))
            .await
            .unwrap(),
        1
    );
    assert_eq!(stored(&dest).await, ["Palermo"]);
    assert_eq!(
        geo_source
            .store_search_to(&dest, from_15_37(), &km200().order(GeoOrder::Asc).count(1))
            .await
            .unwrap(),
        1
    );
    assert_eq!(stored(&dest).await, ["Catania"]);
}

#[tokio::test]
async fn test_radius_store_sorted_order_count() {
    let (source, dest) = store_pair();
    let geo_source = sicily_at(&source).await;
    let desc = km200().order(GeoOrder::Desc).count(1).store_dist();
    assert_eq!(
        geo_source
            .store_search_to(&dest, from_15_37(), &desc)
            .await
            .unwrap(),
        1
    );
    assert_eq!(stored(&dest).await, ["Palermo"]);
    let asc = km200().order(GeoOrder::Asc).count(1).store_dist();
    assert_eq!(
        geo_source
            .store_search_to(&dest, from_15_37(), &asc)
            .await
            .unwrap(),
        1
    );
    assert_eq!(stored(&dest).await, ["Catania"]);
}

#[tokio::test]
async fn test_radius_store_empty() {
    let (source, dest) = store_pair();
    let geo_source = named(&source).await;
    assert_eq!(
        geo_source
            .store_search_to(&dest, from_15_37(), &km200())
            .await
            .unwrap(),
        0
    );
    assert!(stored(&dest).await.is_empty());
}

#[tokio::test]
async fn test_radius_store_member() {
    let (source, dest) = store_pair();
    let geo_source = sicily_at(&source).await;
    assert_eq!(
        geo_source
            .store_search_from_to(&dest, "Palermo", &km200())
            .await
            .unwrap(),
        2
    );
    let mut all = stored(&dest).await;
    all.sort();
    assert_eq!(all, ["Catania", "Palermo"]);
}

#[tokio::test]
async fn test_radius_store_member_count() {
    let (source, dest) = store_pair();
    let geo_source = sicily_at(&source).await;
    assert_eq!(
        geo_source
            .store_search_from_to(&dest, "Palermo", &km200().count(1))
            .await
            .unwrap(),
        1
    );
    assert_eq!(stored(&dest).await, ["Palermo"]);
}

#[tokio::test]
async fn test_radius_store_member_order_count() {
    let (source, dest) = store_pair();
    let geo_source = sicily_at(&source).await;
    assert_eq!(
        geo_source
            .store_search_from_to(&dest, "Palermo", &km200().order(GeoOrder::Desc).count(1))
            .await
            .unwrap(),
        1
    );
    assert_eq!(stored(&dest).await, ["Catania"]);
    assert_eq!(
        geo_source
            .store_search_from_to(&dest, "Palermo", &km200().order(GeoOrder::Asc).count(1))
            .await
            .unwrap(),
        1
    );
    assert_eq!(stored(&dest).await, ["Palermo"]);
}

#[tokio::test]
async fn test_radius_store_member_empty() {
    let (source, dest) = store_pair();
    let geo_source = named(&source).await;
    assert_eq!(
        geo_source
            .store_search_from_to(&dest, "Palermo", &km200())
            .await
            .unwrap(),
        0
    );
    assert!(stored(&dest).await.is_empty());
}

#[tokio::test]
async fn count_any_stops_at_the_first_matches() {
    let geo = sicily().await;
    let found = geo
        .search(from_15_37(), &km200().count_any(1))
        .await
        .unwrap();
    assert_eq!(found.len(), 1);
}
