use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::object::{HasKey, Key};
use crate::reply::{array, bytes, malformed, number, text};
use fred::types::Value;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::marker::PhantomData;

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

enum Origin {
    Point(GeoPoint),
    Member(Value),
}

enum Area {
    Radius(f64),
    Box(f64, f64),
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

    /// Adds a member at a position, or moves it; returns whether the member was new. The member is borrowed: `geo.add("prague", point)`.
    pub async fn add<Q>(&self, v: &Q, at: GeoPoint) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        at.check()?;
        let [longitude, latitude] = at.values();
        let args = vec![
            Value::from(self.key.redis_key()),
            longitude,
            latitude,
            self.member(v)?,
        ];
        let reply = self.key.command("GEOADD", args, 0).await?;
        Ok(number(&reply) == Some(1))
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

    /// Removes every member.
    pub async fn clear(&self) -> Result<()> {
        self.key.del_all(Vec::new()).await?;
        Ok(())
    }

    async fn search(
        &self,
        origin: Origin,
        area: Area,
        unit: GeoUnit,
        count: Option<usize>,
    ) -> Result<Vec<GeoMatch<V>>> {
        let mut args = vec![Value::from(self.key.redis_key())];
        match origin {
            Origin::Point(at) => {
                at.check()?;
                let [longitude, latitude] = at.values();
                args.extend([Value::from("FROMLONLAT"), longitude, latitude]);
            }
            Origin::Member(member) => args.extend([Value::from("FROMMEMBER"), member]),
        }
        match area {
            Area::Radius(radius) => {
                args.extend([Value::from("BYRADIUS"), dimension(radius)?]);
            }
            Area::Box(width, height) => {
                args.extend([Value::from("BYBOX"), dimension(width)?, dimension(height)?]);
            }
        }
        args.extend([Value::from(unit.as_str()), Value::from("ASC")]);
        if let Some(count) = count {
            args.extend([Value::from("COUNT"), crate::reply::int(count)]);
        }
        args.extend([Value::from("WITHDIST"), Value::from("WITHCOORD")]);
        let reply = match self.key.command("GEOSEARCH", args, 0).await {
            Ok(reply) => reply,
            Err(Error::Redis(message))
                if message.contains("could not decode requested zset member") =>
            {
                return Ok(Vec::new());
            }
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

    /// Members within `radius` of a point, nearest first. `count` limits how many are returned.
    pub async fn radius(
        &self,
        center: GeoPoint,
        radius: f64,
        unit: GeoUnit,
        count: Option<usize>,
    ) -> Result<Vec<GeoMatch<V>>> {
        self.search(Origin::Point(center), Area::Radius(radius), unit, count)
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
        let origin = Origin::Member(self.member(member)?);
        self.search(origin, Area::Radius(radius), unit, count).await
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
        self.search(Origin::Point(center), Area::Box(width, height), unit, count)
            .await
    }
}
