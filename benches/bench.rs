use std::{
    any::Any,
    net::TcpListener,
    sync::{
        atomic::{AtomicBool, AtomicU32},
        mpsc, Arc, Condvar, Mutex,
    },
    thread,
    time::Duration,
};

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use kvs::{
    thread_pool::{DefaultThreadPool, ThreadPool},
    KvClient, KvServer, KvStore, KvsEngine,
};
use lazy_static::lazy_static;
use rand::prelude::*;
use tempfile::TempDir;

fn set_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("set_bench");
    group.bench_function("kvs", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                (KvStore::open(temp_dir.path()).unwrap(), temp_dir)
            },
            |(store, _temp_dir)| {
                for i in 1..(1 << 12) {
                    store.set(format!("key{}", i), "value".to_string()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    // group.bench_function("sled", |b| {
    //     b.iter_batched(
    //         || {
    //             let temp_dir = TempDir::new().unwrap();
    //             (SledKvsEngine::new(sled::open(&temp_dir).unwrap()), temp_dir)
    //         },
    //         |(mut db, _temp_dir)| {
    //             for i in 1..(1 << 12) {
    //                 db.set(format!("key{}", i), "value".to_string()).unwrap();
    //             }
    //         },
    //         BatchSize::SmallInput,
    //     )
    // });
    group.finish();
}

fn get_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_bench");
    for i in &vec![8, 12, 16, 20] {
        group.bench_with_input(format!("kvs_{}", i), i, |b, i| {
            let temp_dir = TempDir::new().unwrap();
            let mut store = KvStore::open(temp_dir.path()).unwrap();
            for key_i in 1..(1 << i) {
                store
                    .set(format!("key{}", key_i), "value".to_string())
                    .unwrap();
            }
            let mut rng = SmallRng::from_seed([0; 32]);
            b.iter(|| {
                store
                    .get(format!("key{}", rng.gen_range(1..1 << i)))
                    .unwrap();
            })
        });
    }
    // for i in &vec![8, 12, 16, 20] {
    //     group.bench_with_input(format!("sled_{}", i), i, |b, i| {
    //         let temp_dir = TempDir::new().unwrap();
    //         let mut db = SledKvsEngine::new(sled::open(&temp_dir).unwrap());
    //         for key_i in 1..(1 << i) {
    //             db.set(format!("key{}", key_i), "value".to_string())
    //                 .unwrap();
    //         }
    //         let mut rng = SmallRng::from_seed([0; 16]);
    //         b.iter(|| {
    //             db.get(format!("key{}", rng.gen_range(1, 1 << i))).unwrap();
    //         })
    //     });
    // }
    group.finish();
}

lazy_static! {
    static ref RANGE: Arc<Mutex<(u32, u32)>> = Arc::new(Mutex::new((4000, 6000)));
}

fn find_free_port() -> Option<u32> {
    let mut range = RANGE.lock().unwrap();
    let (start, end) = *range;

    for port in start..=end {
        let addr = format!("127.0.0.1:{}", port);
        if TcpListener::bind(&addr).is_ok() {
            *range = (port + 1, end);
            return Some(port);
        }
    }
    None
}

fn parallel_set_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("parallel_set_bench");
    for i in &[3, 2] {
        group.bench_with_input(format!("kvs_{}", i), i, |b, i| {
            let port = find_free_port().unwrap();
            let addr = format!("127.0.0.1:{}", port);
            let k = 10;
            let temp_dir = TempDir::new().unwrap();

            let thread_pool = DefaultThreadPool::new(*i).unwrap();

            let client_thread_pool = DefaultThreadPool::new(k).unwrap();
            let data = (0..1000)
                .map(|i| (format!("key{}", i), "value".to_string()))
                .collect::<Vec<(String, String)>>();

            let ad = addr.clone();
            thread::spawn(move || {
                let mut server = KvServer::<KvStore, DefaultThreadPool>::new(
                    KvStore::open(temp_dir.path()).unwrap(),
                    thread_pool,
                );
                server.start(ad.as_str()).unwrap();
            });

            let counter = Arc::new(AtomicU32::new(0));
            let pair = Arc::new((Mutex::new(false), Condvar::new()));
            let (lock, cvar) = &*pair;

            // wait server to start
            thread::sleep(Duration::from_secs(1));

            let client_thread_pool = Arc::new(client_thread_pool);
            let data = Arc::new(data);
            let client = KvClient::new(addr);
            let client = Arc::new(client);
            b.iter(|| {
                let client_thread_pool = Arc::clone(&client_thread_pool);

                for i in 0..k {
                    let client = Arc::clone(&client);
                    let data = Arc::clone(&data);
                    let counter = Arc::clone(&counter);
                    let pair = Arc::clone(&pair);

                    client_thread_pool.spawn(move || {
                        let (key, value) = data.get(i as usize).unwrap();
                        client.set(key.clone(), value.clone()).unwrap();
                        counter.fetch_add(1, std::sync::atomic::Ordering::Release);

                        if counter.load(std::sync::atomic::Ordering::Acquire) == k {
                            let (lock, cvar) = &*pair;
                            let mut done = lock.lock().unwrap();
                            *done = true;
                            cvar.notify_one();
                        }
                    });
                }
            });
            let mut done = lock.lock().unwrap();
            while !*done {
                done = cvar.wait(done).unwrap();
            }
        });
    }
    group.finish();
}

fn parallel_get_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("parallel_get_bench");
    for i in &[2, 3] {
        group.bench_with_input(format!("kvs_{}", i), i, |b, i| {
            // println!("start iter {:?}", );
            let id = rand::random::<u32>();
            println!("start iter with id {}", id);
            let port = find_free_port().unwrap();
            let addr = format!("127.0.0.1:{}", port);
            let k = 10;
            let temp_dir = TempDir::new().unwrap();
            let thread_pool = DefaultThreadPool::new(*i).unwrap();

            let client_thread_pool = DefaultThreadPool::new(k).unwrap();
            let data = (0..k)
                .map(|i| (format!("key{}", i), "value".to_string()))
                .collect::<Vec<(String, String)>>();

            let ad = addr.clone();

            thread::spawn(move || {
                let mut server = KvServer::<KvStore, DefaultThreadPool>::new(
                    KvStore::open(temp_dir.path()).unwrap(),
                    thread_pool,
                );
                server.start(ad.as_str()).unwrap();
            });

            // wait server to start
            thread::sleep(Duration::from_secs(1));

            let client = KvClient::new(addr);

            let (end_work_sender, end_work_receiver) = mpsc::channel::<()>();

            println!("start setting data");
            for (key, value) in data.clone() {
                client.set(key, value).unwrap();
            }
            println!("end setting data");

            let counter = Arc::new(AtomicU32::new(0));

            let client_thread_pool = Arc::new(client_thread_pool);
            let data = Arc::new(data);
            let client = Arc::new(client);
            let end_work_sender = Arc::new(end_work_sender);
            b.iter(|| {
                counter.store(0, std::sync::atomic::Ordering::Release);
                let client_thread_pool = Arc::clone(&client_thread_pool);

                for i in 0..k {
                    let client = Arc::clone(&client);
                    let data = Arc::clone(&data);
                    let counter = Arc::clone(&counter);
                    let end_work_sender = Arc::clone(&end_work_sender);

                    client_thread_pool.spawn(move || {
                        let (key, value) = data.get(i as usize).unwrap();
                        match client.get(key.clone()) {
                            Ok(stored_value) => {
                                assert_eq!(Some(value), stored_value.as_ref());
                            }
                            Err(e) => {
                                println!("requesting {key} in {id}");
                                println!("err on id {id} {e}");
                            }
                        }

                        let prev = counter.fetch_add(1, std::sync::atomic::Ordering::AcqRel);

                        if prev + 1 == k {
                            end_work_sender.send(()).unwrap();
                        }
                    });
                }
                end_work_receiver.recv().unwrap();
            });
        });
    }
    group.finish();
}

criterion_group!(benches, parallel_get_bench);
//criterion_group!(benches, set_bench, get_bench);
criterion_main!(benches);
