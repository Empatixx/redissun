use crate::codec::Codec;
use crate::core::no_retry;
use crate::error::{Error, Result};
use crate::object::{HasKey, Key};
use crate::reply::{array, bytes, malformed, number, text};
use fred::interfaces::ClientLike;
use fred::types::scripts::Script;
use fred::types::{ClusterHash, CustomCommand, Value};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::marker::PhantomData;
use std::sync::LazyLock;

static ADD_IF_EXISTS: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local value = redis.call('GEOPOS', KEYS[1], ARGV[3])
        if value[1] ~= false then
            redis.call('GEOADD', KEYS[1], ARGV[1], ARGV[2], ARGV[3])
            return 1
        end
        return 0",
    )
});

const MAX_LATITUDE: f64 = 85.05112878;

/// A position on Earth in degrees.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GeoPoint {
    /// Degrees east of the prime meridian, from -180 to 180.
    pub longitude: f64,
    /// Degrees north of the equator, from -85.05112878 to 85.05112878.
    pub latitude: f64,
}

impl GeoPoint {
    fn check(&self) -> Result<()> {
        let valid = self.longitude.is_finite()
            && self.latitude.is_finite()
            && self.longitude.abs() <= 180.0
            && self.latitude.abs() <= MAX_LATITUDE;
        if valid {
            Ok(())
        } else {
            Err(Error::Config(
                "longitude must be within 180 and latitude within 85.05112878 degrees".into(),
            ))
        }
    }

    fn values(&self) -> [Value; 2] {
        [
            Value::from(self.longitude.to_string()),
            Value::from(self.latitude.to_string()),
        ]
    }
}

/// A unit of distance.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum GeoUnit {
    /// Meters.
    #[default]
    Meters,
    /// Kilometers.
    Kilometers,
    /// Miles.
    Miles,
    /// Feet.
    Feet,
}

impl GeoUnit {
    fn as_str(self) -> &'static str {
        match self {
            GeoUnit::Meters => "m",
            GeoUnit::Kilometers => "km",
            GeoUnit::Miles => "mi",
            GeoUnit::Feet => "ft",
        }
    }
}

/// A member found by a search, with its distance from the search origin and its position.
#[derive(Clone, Debug, PartialEq)]
pub struct GeoMatch<V> {
    /// The member.
    pub value: V,
    /// Distance from the origin, in the unit of the search.
    pub distance: f64,
    /// Where the member is.
    pub point: GeoPoint,
}

/// The order of search results by distance from the origin.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GeoOrder {
    /// Nearest first.
    Asc,
    /// Farthest first.
    Desc,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Area {
    Radius(f64),
    Box(f64, f64),
}

/// The shape, order and limit of a search, like Redisson's `GeoSearchArgs` without the origin. The origin is given to [`Geo::search`] or [`Geo::search_from`].
///
/// ```ignore
/// let args = GeoSearch::radius(200.0, GeoUnit::Kilometers).order(GeoOrder::Asc).count(1);
/// let nearest = geo.search(point, &args).await?;
/// ```
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GeoSearch {
    area: Area,
    unit: GeoUnit,
    order: Option<GeoOrder>,
    count: Option<usize>,
    any: bool,
    store_dist: bool,
}

impl GeoSearch {
    fn new(area: Area, unit: GeoUnit) -> Self {
        Self {
            area,
            unit,
            order: None,
            count: None,
            any: false,
            store_dist: false,
        }
    }

    /// Members within `radius` of the origin (`BYRADIUS`).
    pub fn radius(radius: f64, unit: GeoUnit) -> Self {
        Self::new(Area::Radius(radius), unit)
    }

    /// Members inside a `width` by `height` rectangle centered on the origin (`BYBOX`).
    pub fn rect(width: f64, height: f64, unit: GeoUnit) -> Self {
        Self::new(Area::Box(width, height), unit)
    }

    /// Sorts the results by distance. Without it Redis picks the order, except that a `count` returns the nearest members.
    pub fn order(mut self, order: GeoOrder) -> Self {
        self.order = Some(order);
        self
    }

    /// Returns at most `count` members.
    pub fn count(mut self, count: usize) -> Self {
        self.count = Some(count);
        self.any = false;
        self
    }

    /// Returns at most `count` members, stopping as soon as that many are found rather than the nearest ones (`COUNT ANY`).
    pub fn count_any(mut self, count: usize) -> Self {
        self.count = Some(count);
        self.any = true;
        self
    }

    /// For the `store_*` methods only: stores the distance as the score instead of the geohash, like Redisson's `storeSortedSearchTo`. The destination is then a plain sorted set ordered by distance.
    pub fn store_dist(mut self) -> Self {
        self.store_dist = true;
        self
    }

    fn args(&self, args: &mut Vec<Value>) -> Result<()> {
        match self.area {
            Area::Radius(radius) => {
                args.extend([Value::from("BYRADIUS"), dimension(radius)?]);
            }
            Area::Box(width, height) => {
                args.extend([Value::from("BYBOX"), dimension(width)?, dimension(height)?]);
            }
        }
        args.push(Value::from(self.unit.as_str()));
        match self.order {
            Some(GeoOrder::Asc) => args.push(Value::from("ASC")),
            Some(GeoOrder::Desc) => args.push(Value::from("DESC")),
            None => {}
        }
        if let Some(count) = self.count {
            args.extend([Value::from("COUNT"), crate::reply::int(count)]);
            if self.any {
                args.push(Value::from("ANY"));
            }
        }
        Ok(())
    }
}

enum Origin {
    Point(GeoPoint),
    Member(Value),
}

impl Origin {
    fn args(self, args: &mut Vec<Value>) -> Result<()> {
        match self {
            Origin::Point(at) => {
                at.check()?;
                let [longitude, latitude] = at.values();
                args.extend([Value::from("FROMLONLAT"), longitude, latitude]);
            }
            Origin::Member(member) => args.extend([Value::from("FROMMEMBER"), member]),
        }
        Ok(())
    }
}

fn missing_member(error: &Error) -> bool {
    matches!(error, Error::Redis(message) if message.contains("could not decode requested zset member"))
}

fn dimension(size: f64) -> Result<Value> {
    if size.is_finite() && size >= 0.0 {
        Ok(Value::from(size.to_string()))
    } else {
        Err(Error::Config("a search size must be zero or more".into()))
    }
}

fn float(value: &Value) -> Option<f64> {
    text(value)?.parse().ok()
}

fn point(value: &Value) -> Option<GeoPoint> {
    match array(value) {
        [longitude, latitude] => Some(GeoPoint {
            longitude: float(longitude)?,
            latitude: float(latitude)?,
        }),
        _ => None,
    }
}

/// A distributed set of members with a position on Earth, stored in a Redis sorted set, as in Redisson's `RGeo`. Members are equal when their encoded bytes are equal. Positions are kept with a precision of about 0.6 millimeters.
pub struct Geo<V, C: Codec> {
    key: Key,
    codec: C,
    _marker: PhantomData<fn() -> V>,
}

impl<V, C: Codec> Clone for Geo<V, C> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            codec: self.codec.clone(),
            _marker: PhantomData,
        }
    }
}

impl<V, C: Codec> fmt::Debug for Geo<V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "Geo")
    }
}

impl<V, C: Codec> HasKey for Geo<V, C> {
    fn key(&self) -> &Key {
        &self.key
    }
}

impl<V, C: Codec> Geo<V, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        Self {
            key,
            codec,
            _marker: PhantomData,
        }
    }
}

impl<V, C> Geo<V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    fn member<Q>(&self, v: &Q) -> Result<Value>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        Ok(Value::Bytes(self.codec.encode(v)?))
    }

    async fn add_command(&self, args: Vec<Value>) -> Result<Value> {
        let command = CustomCommand::new_static("GEOADD", ClusterHash::Offset(0), false);
        Ok(no_retry(self.key.core.redis())
            .custom(command, args)
            .await?)
    }

    async fn add_entries<'a, Q>(
        &self,
        flag: Option<&'static str>,
        entries: impl IntoIterator<Item = (&'a Q, GeoPoint)>,
    ) -> Result<usize>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync + 'a,
    {
        let mut args = vec![Value::from(self.key.redis_key())];
        args.extend(flag.map(Value::from));
        let first = args.len();
        for (v, at) in entries {
            at.check()?;
            args.extend(at.values());
            args.push(self.member(v)?);
        }
        if args.len() == first {
            return Ok(0);
        }
        let reply = self.add_command(args).await?;
        Ok(number(&reply).unwrap_or(0) as usize)
    }

    /// Adds a member at a position, or moves it (`GEOADD`, never retried); returns whether the member was new. The member is borrowed: `geo.add("prague", point)`.
    pub async fn add<Q>(&self, v: &Q, at: GeoPoint) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        Ok(self.add_entries(None, [(v, at)]).await? == 1)
    }

    /// Adds or moves many members with one `GEOADD`; returns how many were new.
    pub async fn extend<'a, Q>(
        &self,
        entries: impl IntoIterator<Item = (&'a Q, GeoPoint)>,
    ) -> Result<usize>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync + 'a,
    {
        self.add_entries(None, entries).await
    }

    /// Adds a member only when it is missing (`GEOADD NX`); returns whether it was added. Redisson's `tryAdd`.
    pub async fn try_add<Q>(&self, v: &Q, at: GeoPoint) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        Ok(self.add_entries(Some("NX"), [(v, at)]).await? == 1)
    }

    /// Moves a member only when it is already there; returns whether it was.
    pub async fn add_if_exists<Q>(&self, v: &Q, at: GeoPoint) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        at.check()?;
        let moved: i64 = self
            .key
            .core
            .eval(
                &ADD_IF_EXISTS,
                vec![self.key.redis_key()],
                vec![
                    bytes::Bytes::from(at.longitude.to_string()),
                    bytes::Bytes::from(at.latitude.to_string()),
                    self.codec.encode(v)?,
                ],
            )
            .await?;
        Ok(moved == 1)
    }

    /// Removes a member; returns whether it was there.
    pub async fn remove<Q>(&self, v: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let args = vec![Value::from(self.key.redis_key()), self.member(v)?];
        let reply = self.key.command("ZREM", args, 0).await?;
        Ok(number(&reply) == Some(1))
    }

    /// Returns the position of a member.
    pub async fn pos<Q>(&self, v: &Q) -> Result<Option<GeoPoint>>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let args = vec![Value::from(self.key.redis_key()), self.member(v)?];
        let reply = self.key.command("GEOPOS", args, 0).await?;
        Ok(array(&reply).first().and_then(point))
    }

    /// Returns the geohash string of a member, which is 11 characters long.
    pub async fn hash<Q>(&self, v: &Q) -> Result<Option<String>>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let args = vec![Value::from(self.key.redis_key()), self.member(v)?];
        let reply = self.key.command("GEOHASH", args, 0).await?;
        Ok(array(&reply).first().and_then(text))
    }

    /// Positions of several members in the order given, `None` for a missing one (`GEOPOS`).
    pub async fn pos_many<Q>(&self, members: &[&Q]) -> Result<Vec<Option<GeoPoint>>>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let reply = self
            .key
            .command("GEOPOS", self.members(members)?, 0)
            .await?;
        Ok(array(&reply).iter().map(point).collect())
    }

    /// Geohash strings of several members in the order given, `None` for a missing one (`GEOHASH`).
    pub async fn hash_many<Q>(&self, members: &[&Q]) -> Result<Vec<Option<String>>>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let reply = self
            .key
            .command("GEOHASH", self.members(members)?, 0)
            .await?;
        Ok(array(&reply).iter().map(text).collect())
    }

    fn members<Q>(&self, members: &[&Q]) -> Result<Vec<Value>>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        std::iter::once(Ok(Value::from(self.key.redis_key())))
            .chain(members.iter().map(|v| self.member(*v)))
            .collect()
    }

    /// Distance between two members, or `None` when one of them is missing.
    pub async fn dist<Q>(&self, from: &Q, to: &Q, unit: GeoUnit) -> Result<Option<f64>>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let args = vec![
            Value::from(self.key.redis_key()),
            self.member(from)?,
            self.member(to)?,
            Value::from(unit.as_str()),
        ];
        let reply = self.key.command("GEODIST", args, 0).await?;
        Ok(float(&reply))
    }

    /// Number of members.
    pub async fn len(&self) -> Result<usize> {
        let reply = self
            .key
            .command("ZCARD", vec![Value::from(self.key.redis_key())], 0)
            .await?;
        Ok(number(&reply).unwrap_or(0) as usize)
    }

    /// Returns whether there are no members.
    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    /// All members in score order, which for a geo set is geohash order (`ZRANGE 0 -1`). After a store with [`GeoSearch::store_dist`] it is nearest first. Redisson's `readAll`.
    pub async fn read_all(&self) -> Result<Vec<V>> {
        let args = vec![
            Value::from(self.key.redis_key()),
            Value::Integer(0),
            Value::Integer(-1),
        ];
        let reply = self.key.command("ZRANGE", args, 0).await?;
        array(&reply)
            .iter()
            .map(|member| self.codec.decode(&bytes(member).ok_or_else(malformed)?))
            .collect()
    }

    /// Removes every member.
    pub async fn clear(&self) -> Result<()> {
        self.key.del_all(Vec::new()).await?;
        Ok(())
    }

    async fn run_search(&self, origin: Origin, args: &GeoSearch) -> Result<Vec<GeoMatch<V>>> {
        if args.count == Some(0) {
            return Ok(Vec::new());
        }
        let mut command = vec![Value::from(self.key.redis_key())];
        origin.args(&mut command)?;
        args.args(&mut command)?;
        command.extend([Value::from("WITHDIST"), Value::from("WITHCOORD")]);
        let reply = match self.key.command("GEOSEARCH", command, 0).await {
            Ok(reply) => reply,
            Err(error) if missing_member(&error) => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        array(&reply)
            .iter()
            .map(|row| match array(row) {
                [member, distance, position] => Ok(GeoMatch {
                    value: self.codec.decode(&bytes(member).ok_or_else(malformed)?)?,
                    distance: float(distance).ok_or_else(malformed)?,
                    point: point(position).ok_or_else(malformed)?,
                }),
                _ => Err(malformed()),
            })
            .collect()
    }

    async fn run_store(
        &self,
        destination: &str,
        origin: Origin,
        args: &GeoSearch,
    ) -> Result<usize> {
        let mut command = vec![
            Value::from(destination.to_string()),
            Value::from(self.key.redis_key()),
        ];
        origin.args(&mut command)?;
        args.args(&mut command)?;
        if args.store_dist {
            command.push(Value::from("STOREDIST"));
        }
        let reply = match self.key.command("GEOSEARCHSTORE", command, 0).await {
            Ok(reply) => reply,
            Err(error) if missing_member(&error) => return Ok(0),
            Err(error) => return Err(error),
        };
        Ok(number(&reply).unwrap_or(0) as usize)
    }

    /// Members found around a point (`GEOSEARCH FROMLONLAT`), each with its distance in the search unit and its position. Redisson's `search`, `searchWithDistance` and `searchWithPosition` in one.
    pub async fn search(&self, center: GeoPoint, args: &GeoSearch) -> Result<Vec<GeoMatch<V>>> {
        self.run_search(Origin::Point(center), args).await
    }

    /// Members found around another member (`GEOSEARCH FROMMEMBER`). The member itself is included, at distance 0. An unknown member gives an empty list.
    pub async fn search_from<Q>(&self, member: &Q, args: &GeoSearch) -> Result<Vec<GeoMatch<V>>>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.run_search(Origin::Member(self.member(member)?), args)
            .await
    }

    /// Stores the members found around a point in the geo set `destination`, replacing it (`GEOSEARCHSTORE`); returns how many. Redisson's `storeSearchTo`, or `storeSortedSearchTo` with [`GeoSearch::store_dist`]. In Redis Cluster both names must share a hash slot.
    pub async fn store_search_to(
        &self,
        destination: &str,
        center: GeoPoint,
        args: &GeoSearch,
    ) -> Result<usize> {
        self.run_store(destination, Origin::Point(center), args)
            .await
    }

    /// Stores the members found around another member in the geo set `destination`, replacing it (`GEOSEARCHSTORE`); returns how many.
    pub async fn store_search_from_to<Q>(
        &self,
        destination: &str,
        member: &Q,
        args: &GeoSearch,
    ) -> Result<usize>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.run_store(destination, Origin::Member(self.member(member)?), args)
            .await
    }

    fn nearest(area: GeoSearch, count: Option<usize>) -> GeoSearch {
        let args = area.order(GeoOrder::Asc);
        match count {
            Some(count) => args.count(count),
            None => args,
        }
    }

    /// Members within `radius` of a point, nearest first. `count` limits how many are returned.
    pub async fn radius(
        &self,
        center: GeoPoint,
        radius: f64,
        unit: GeoUnit,
        count: Option<usize>,
    ) -> Result<Vec<GeoMatch<V>>> {
        self.search(
            center,
            &Self::nearest(GeoSearch::radius(radius, unit), count),
        )
        .await
    }

    /// Members within `radius` of another member, nearest first. The member itself is included, at distance 0. An unknown member gives an empty list.
    pub async fn radius_of<Q>(
        &self,
        member: &Q,
        radius: f64,
        unit: GeoUnit,
        count: Option<usize>,
    ) -> Result<Vec<GeoMatch<V>>>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.search_from(
            member,
            &Self::nearest(GeoSearch::radius(radius, unit), count),
        )
        .await
    }

    /// Members inside a rectangle of `width` by `height` around a point, nearest first.
    pub async fn within_box(
        &self,
        center: GeoPoint,
        width: f64,
        height: f64,
        unit: GeoUnit,
        count: Option<usize>,
    ) -> Result<Vec<GeoMatch<V>>> {
        self.search(
            center,
            &Self::nearest(GeoSearch::rect(width, height, unit), count),
        )
        .await
    }
}
