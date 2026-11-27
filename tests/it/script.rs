use crate::common::{client, unique};
use bytes::Bytes;
use redissun::{Decoded, Error, LuaScript, Object, ScriptMode, StringCodec};
use tokio::sync::Mutex;

static CACHE: Mutex<()> = Mutex::const_new(());
static FUNCTIONS: Mutex<()> = Mutex::const_new(());

const GET_FOO: &str = "return redis.call('get', 'foo')";
const GET_FOO_SHA: &str = "282297a0228f48cd3fc6a55de6316f31422f5d17";

const INCRBY: &str = "return redis.call('INCRBY', KEYS[1], ARGV[1])";

#[tokio::test]
async fn eval_runs_a_script_with_keys_and_arguments() {
    let script = client().await.with_codec(StringCodec).script();
    let key = unique("counter");
    let first: i64 = script.eval(INCRBY).key(key.clone()).arg(&5).await.unwrap();
    let second: i64 = script.eval(INCRBY).key(key).arg(&2).await.unwrap();
    assert_eq!((first, second), (5, 7));
}

#[tokio::test]
async fn arguments_and_values_go_through_the_codec() {
    let client = client().await;
    let bucket = client.bucket::<Vec<String>>(unique("bucket"));
    bucket
        .set(&vec!["a".to_string(), "b".to_string()])
        .await
        .unwrap();
    let script = client.script();

    let echoed: Decoded<Vec<String>> = script
        .eval("return ARGV[1]")
        .arg(&vec!["x", "y"])
        .await
        .unwrap();
    assert_eq!(echoed.into_inner(), vec!["x", "y"]);

    let stored: Option<Decoded<Vec<String>>> = script
        .eval("return redis.call('GET', KEYS[1])")
        .key(bucket.name())
        .await
        .unwrap();
    assert_eq!(stored.unwrap().0, vec!["a", "b"]);

    let raw: String = script.eval("return ARGV[1]").arg("text").await.unwrap();
    assert_eq!(raw, "\"text\"");
}

#[tokio::test]
async fn replies_become_rust_types() {
    let script = client().await.with_codec(StringCodec).script();
    let yes: bool = script.eval("return 1").await.unwrap();
    let no: bool = script.eval("return false").await.unwrap();
    let nothing: Option<String> = script.eval("return nil").await.unwrap();
    let status: String = script
        .eval("return redis.status_reply('PONG')")
        .await
        .unwrap();
    let list: Vec<String> = script.eval("return {'a', 'b'}").await.unwrap();
    let numbers: Vec<i64> = script.eval("return {1, 2, 3}").await.unwrap();
    let float: f64 = script.eval("return tostring(1.5)").await.unwrap();
    let bytes: Bytes = script
        .eval("return ARGV[1]")
        .arg_bytes(vec![0u8, 255])
        .await
        .unwrap();
    script.eval::<()>("return 1").await.unwrap();
    assert!(yes);
    assert!(!no);
    assert_eq!(nothing, None);
    assert_eq!(status, "PONG");
    assert_eq!(list, ["a", "b"]);
    assert_eq!(numbers, [1, 2, 3]);
    assert_eq!(float, 1.5);
    assert_eq!(&bytes[..], &[0, 255]);
}

#[tokio::test]
async fn a_script_error_is_a_redis_error() {
    let script = client().await.script();
    let outcome = script.eval::<()>("return redis.error_reply('boom')").await;
    assert!(matches!(outcome, Err(Error::Redis(message)) if message.contains("boom")));
    let wrong: redissun::Result<i64> = script.eval("return {}").await;
    assert!(wrong.is_err());
}

#[tokio::test]
async fn eval_sends_the_source_only_when_redis_lacks_the_script() {
    let _cache = CACHE.lock().await;
    let script = client().await.with_codec(StringCodec).script();
    let source = format!("return '{}'", unique("cached"));
    let lua = LuaScript::new(source.clone());
    script.flush().await.unwrap();
    assert_eq!(script.exists(&[lua.sha1()]).await.unwrap(), [false]);

    let reply: String = script.eval(&source).await.unwrap();
    assert!(reply.starts_with("cached"));
    assert_eq!(script.exists(&[lua.sha1()]).await.unwrap(), [true]);
}

#[tokio::test]
async fn eval_sha_runs_a_loaded_script() {
    let _cache = CACHE.lock().await;
    let script = client().await.with_codec(StringCodec).script();
    let source = format!("return ARGV[1] .. '{}'", unique("loaded"));
    let sha = script.load(&source).await.unwrap();
    assert_eq!(sha, LuaScript::new(source).sha1());
    let reply: String = script.eval_sha(&sha).arg("x").await.unwrap();
    assert!(reply.starts_with("xloaded"));

    let missing = script
        .eval_sha::<String>("0000000000000000000000000000000000000000")
        .await;
    assert!(matches!(missing, Err(Error::Redis(message)) if message.contains("NOSCRIPT")));
}

#[tokio::test]
async fn a_lua_script_reloads_itself_after_a_flush() {
    let _cache = CACHE.lock().await;
    let script = client().await.with_codec(StringCodec).script();
    let lua = LuaScript::new(INCRBY);
    let key = unique("counter");
    let first: i64 = script.run(&lua).key(key.clone()).arg(&1).await.unwrap();
    script.flush().await.unwrap();
    let second: i64 = script.run(&lua).key(key).arg(&1).await.unwrap();
    assert_eq!((first, second), (1, 2));
    assert!(format!("{lua:?}").contains(lua.sha1()));
}

#[tokio::test]
async fn exists_answers_for_each_digest() {
    let _cache = CACHE.lock().await;
    let script = client().await.script();
    let sha = script.load("return 'exists'").await.unwrap();
    let found = script
        .exists(&[&sha, "0000000000000000000000000000000000000000"])
        .await
        .unwrap();
    assert_eq!(found, [true, false]);
    assert!(script.exists(&[]).await.unwrap().is_empty());
}

#[tokio::test]
async fn read_only_scripts_can_read_but_not_write() {
    let script = client().await.with_codec(StringCodec).script();
    let key = unique("ro");
    let _: i64 = script.eval(INCRBY).key(key.clone()).arg(&3).await.unwrap();

    let read: i64 = script
        .eval("return tonumber(redis.call('GET', KEYS[1]))")
        .key(key.clone())
        .read_only()
        .await
        .unwrap();
    assert_eq!(read, 3);

    let write = script
        .eval::<i64>(INCRBY)
        .key(key.clone())
        .arg(&1)
        .mode(ScriptMode::ReadOnly)
        .await;
    assert!(write.is_err());

    let lua = LuaScript::new("return redis.call('EXISTS', KEYS[1])");
    let exists: bool = script.run(&lua).key(key).read_only().await.unwrap();
    assert!(exists);
}

#[tokio::test]
async fn kill_without_a_running_script_fails() {
    let script = client().await.script();
    let outcome = script.kill().await;
    assert!(matches!(outcome, Err(Error::Redis(message)) if message.contains("NOTBUSY")));
}

#[tokio::test]
async fn an_argument_the_codec_refuses_fails_the_call() {
    #[derive(serde::Serialize)]
    struct Pair {
        a: i32,
    }
    let script = client().await.with_codec(StringCodec).script();
    let outcome = script.eval::<()>("return 1").arg(&Pair { a: 1 }).await;
    assert!(matches!(outcome, Err(Error::Codec(_))));
}

fn library(name: &str) -> String {
    format!(
        "#!lua name={name}\n\
         redis.register_function('{name}_incr', function(keys, args) return redis.call('INCRBY', keys[1], args[1]) end)\n\
         redis.register_function{{function_name='{name}_get', callback=function(keys, args) return redis.call('GET', keys[1]) end, flags={{'no-writes'}}}}"
    )
}

#[tokio::test]
async fn functions_load_call_and_delete() {
    let _functions = FUNCTIONS.lock().await;
    let functions = client().await.with_codec(StringCodec).function();
    let name = unique("lib").replace([':', '-'], "_");
    let code = library(&name);
    assert_eq!(functions.load(&code).await.unwrap(), name);
    assert!(functions.load(&code).await.is_err());
    assert_eq!(functions.load_and_replace(&code).await.unwrap(), name);

    let key = unique("fcall");
    let total: i64 = functions
        .call(&format!("{name}_incr"))
        .key(key.clone())
        .arg(&4)
        .await
        .unwrap();
    assert_eq!(total, 4);
    let read: i64 = functions
        .call(&format!("{name}_get"))
        .key(key.clone())
        .read_only()
        .await
        .unwrap();
    assert_eq!(read, 4);
    let refused = functions
        .call::<i64>(&format!("{name}_incr"))
        .key(key)
        .arg(&1)
        .read_only()
        .await;
    assert!(refused.is_err());

    functions.delete(&name).await.unwrap();
    let gone = functions.call::<i64>(&format!("{name}_get")).key("k").await;
    assert!(gone.is_err());
}

#[tokio::test]
async fn function_kill_without_a_running_function_fails() {
    let functions = client().await.function();
    assert!(functions.kill().await.is_err());
}

#[tokio::test]
async fn a_read_only_script_returns_nested_lists() {
    let script = client().await.with_codec(StringCodec).script();
    let key = unique("ABCD17436");
    for stamp in 1506524856000_i64..1506524856100 {
        let member = format!("DENY\tTESTREDISSON\t{stamp}\thelloworld_hongqin");
        let _: i64 = script
            .eval("return redis.call('zadd', KEYS[1], 0, ARGV[1])")
            .key(key.clone())
            .arg(&member)
            .await
            .unwrap();
    }

    let max = "'[DENY\tTESTREDISSON\t1506524856099'";
    let min = "'[DENY\tTESTREDISSON\t1506524856000'";
    let source = format!(
        "local d = {{}}; d[1] = redis.call('zrevrangebylex','{key}',{max},{min},'LIMIT',0,5);  \
         d[2] = redis.call('zrevrangebylex','{key}',{max},{min},'LIMIT',0,15);  \
         d[3] = redis.call('zrevrangebylex','{key}',{max},{min},'LIMIT',0,25);  return d;"
    );
    let lists: Vec<Vec<String>> = script.eval(&source).read_only().await.unwrap();
    assert_eq!(lists.len(), 3);
    assert_eq!(lists[0].len(), 5);
    assert_eq!(lists[1].len(), 15);
    assert_eq!(lists[2].len(), 25);
}

#[tokio::test]
async fn a_read_only_script_returns_a_string() {
    let script = client().await.with_codec(StringCodec).script();
    let reply: String = script.eval("return 'hello'").read_only().await.unwrap();
    assert_eq!(reply, "hello");
}

#[tokio::test]
async fn a_list_reply_ends_at_the_first_nil() {
    let script = client().await.with_codec(StringCodec).script();
    let reply: Vec<String> = script
        .eval("return {'1','2','3.3333','foo',nil,'bar'}")
        .read_only()
        .await
        .unwrap();
    assert_eq!(reply, ["1", "2", "3.3333", "foo"]);
}

#[tokio::test]
async fn a_value_set_by_a_script_is_read_back_through_the_codec() {
    let script = client().await.script();
    let key = unique("foo");
    let value = "test".to_string();
    let _: Option<Decoded<String>> = script
        .eval("redis.call('set', KEYS[1], ARGV[1])")
        .key(key.clone())
        .arg(&value)
        .await
        .unwrap();

    let read: Decoded<String> = script
        .eval("return redis.call('get', KEYS[1])")
        .key(key)
        .await
        .unwrap();
    assert_eq!(read.into_inner(), value);
}

#[tokio::test]
async fn exists_is_false_after_a_flush() {
    let _cache = CACHE.lock().await;
    let script = client().await.script();
    let sha = script.load(GET_FOO).await.unwrap();
    assert_eq!(sha, GET_FOO_SHA);

    assert_eq!(script.exists(&[&sha]).await.unwrap(), [true]);

    script.flush().await.unwrap();

    assert_eq!(script.exists(&[&sha]).await.unwrap(), [false]);
}

#[tokio::test]
async fn eval_sha_fails_with_noscript_after_a_flush() {
    let _cache = CACHE.lock().await;
    let client = client().await;
    client
        .bucket::<String>("foo")
        .set(&"bar".to_string())
        .await
        .unwrap();
    let script = client.script();
    assert_eq!(script.load(GET_FOO).await.unwrap(), GET_FOO_SHA);
    let read: Decoded<String> = script.eval_sha(GET_FOO_SHA).read_only().await.unwrap();
    assert_eq!(read.into_inner(), "bar");
    script.flush().await.unwrap();

    let outcome = script
        .eval_sha::<Decoded<String>>(GET_FOO_SHA)
        .read_only()
        .await;
    assert!(matches!(outcome, Err(Error::Redis(message)) if message.contains("NOSCRIPT")));
}

#[tokio::test]
async fn load_returns_the_digest_that_eval_sha_runs() {
    let _cache = CACHE.lock().await;
    let client = client().await;
    client
        .bucket::<String>("foo")
        .set(&"bar".to_string())
        .await
        .unwrap();
    let script = client.script();
    assert_eq!(script.load(GET_FOO).await.unwrap(), GET_FOO_SHA);
    let read: Decoded<String> = script.eval_sha(GET_FOO_SHA).read_only().await.unwrap();
    assert_eq!(read.into_inner(), "bar");
}

#[tokio::test]
async fn eval_sha_reads_a_value_stored_after_the_script_was_loaded() {
    let _cache = CACHE.lock().await;
    let client = client().await;
    let script = client.script();
    assert_eq!(script.load(GET_FOO).await.unwrap(), GET_FOO_SHA);

    client
        .bucket::<String>("foo")
        .set(&"bar".to_string())
        .await
        .unwrap();
    let read: Decoded<String> = script.eval_sha(GET_FOO_SHA).read_only().await.unwrap();
    assert_eq!(read.into_inner(), "bar");
}

#[tokio::test]
async fn eval_and_eval_sha_of_one_script_return_the_same_value() {
    let _cache = CACHE.lock().await;
    let client = client().await;
    let script = client.script();
    assert_eq!(script.load(GET_FOO).await.unwrap(), GET_FOO_SHA);

    client
        .bucket::<String>("foo")
        .set(&"bar".to_string())
        .await
        .unwrap();
    let evaluated: Decoded<String> = script.eval(GET_FOO).read_only().await.unwrap();
    assert_eq!(evaluated.into_inner(), "bar");
    let by_digest: Decoded<String> = script.eval_sha(GET_FOO_SHA).read_only().await.unwrap();
    assert_eq!(by_digest.into_inner(), "bar");
}

fn named_library(name: &str, code: &str) -> String {
    format!("#!lua name={name} \n {code}")
}

#[tokio::test]
async fn functions_are_called_in_read_and_write_mode_and_replaced() {
    let _functions = FUNCTIONS.lock().await;
    let client = client().await;
    let functions = client.function();
    functions.flush().await.unwrap();
    functions
        .load(&named_library(
            "lib",
            "redis.register_function{function_name='myfun', callback=function(keys, args) return args[1] end, flags={ 'no-writes' }}\
             redis.register_function{function_name='myfun2', callback=function(keys, args) return 'test' end, flags={ 'no-writes' }}\
             redis.register_function('myfun3', function(keys, args) return 123 end)",
        ))
        .await
        .unwrap();
    let echoed: Decoded<String> = functions
        .call("myfun")
        .read_only()
        .arg("test")
        .await
        .unwrap();
    assert_eq!(echoed.into_inner(), "test");

    let text = client.with_codec(StringCodec).function();
    let constant: String = text.call("myfun2").read_only().await.unwrap();
    assert_eq!(constant, "test");

    let number: i64 = text.call("myfun3").await.unwrap();
    assert_eq!(number, 123);

    functions
        .load_and_replace(&named_library(
            "lib",
            "redis.register_function('myfun', function(keys, args) return args[1] end)\
             redis.register_function('myfun2', function(keys, args) return 'test2' end)\
             redis.register_function('myfun3', function(keys, args) return 123 end)",
        ))
        .await
        .unwrap();

    let replaced: String = text.call("myfun2").await.unwrap();
    assert_eq!(replaced, "test2");
}

#[tokio::test]
async fn a_function_receives_its_keys_and_arguments() {
    let _functions = FUNCTIONS.lock().await;
    let client = client().await;
    let functions = client.function();
    functions.flush().await.unwrap();
    functions
        .load(&named_library(
            "lib",
            "redis.register_function{function_name='myfun', callback=function(keys, args) return keys[1] end, flags={ 'no-writes' }}\
             redis.register_function('myfun2', function(keys, args) return args[1] end)",
        ))
        .await
        .unwrap();
    let key: String = functions
        .call("myfun")
        .read_only()
        .key("testKey")
        .arg("arg1")
        .await
        .unwrap();
    assert_eq!(key, "testKey");

    let text = client.with_codec(StringCodec).function();
    let argument: String = text
        .call("myfun2")
        .keys(["testKey1", "testKey2"])
        .arg("arg1")
        .await
        .unwrap();
    assert_eq!(argument, "arg1");

    let decoded: Decoded<String> = functions
        .call("myfun2")
        .key("testKey")
        .arg("argv1")
        .await
        .unwrap();
    assert_eq!(decoded.into_inner(), "argv1");
}

#[tokio::test]
async fn a_flushed_script_is_loaded_again() {
    let map = client().await.hash_map::<String, i64>(unique("flushed"));
    assert_eq!(map.insert("a", &1).await.unwrap(), None);
    assert!(crate::common::raw_command(&["SCRIPT", "FLUSH"])
        .await
        .starts_with("+OK"));
    assert_eq!(map.insert("a", &2).await.unwrap(), Some(1));
}
