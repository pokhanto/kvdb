use std::{
    net::TcpListener,
    sync::{atomic::AtomicU32, mpsc, Arc},
    thread,
    time::Duration,
};

use console_subscriber::ConsoleLayer;
use criterion::{criterion_group, criterion_main, Criterion};
use kvs::{thread_pool::ThreadPool, KvClient, KvServer, KvServerAsync, KvStore, KvStoreAsync};
use tempfile::TempDir;
use tokio::runtime::Builder;

const SAMPLE_SIZE: usize = 50;
const MEASUREMENT_TIME_SEC: u64 = 30;
const ENTRIES_NUMBER: u32 = 600;
const CLINET_THREADS_NUMBER: u32 = ENTRIES_NUMBER / 60;

fn find_free_port() -> Option<u32> {
    let (start, end) = (4000, 6000);

    for port in start..=end {
        let addr = format!("127.0.0.1:{}", port);
        if TcpListener::bind(&addr).is_ok() {
            return Some(port);
        }
    }

    None
}

fn parallel_get_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("parallel get");
    group.sample_size(SAMPLE_SIZE);
    group.measurement_time(Duration::from_secs(MEASUREMENT_TIME_SEC));

    for thread_number in &[1, 2, 4, 8, 16] {
        group.bench_with_input(
            format!("{}-thread threadpool", thread_number),
            thread_number,
            |b, thread_number| {
                let port = find_free_port().unwrap();
                let addr = format!("127.0.0.1:{}", port);
                let temp_dir = TempDir::new().unwrap();
                let thread_pool = ThreadPool::new(*thread_number).unwrap();

                let client_thread_pool = ThreadPool::new(CLINET_THREADS_NUMBER).unwrap();
                let data = (0..ENTRIES_NUMBER)
                    .map(|i| (format!("key{}", i), "value".to_string()))
                    .collect::<Vec<(String, String)>>();

                let ad = addr.clone();

                thread::spawn(move || {
                    let mut server = KvServer::<KvStore>::new(
                        KvStore::open(temp_dir.path()).unwrap(),
                        thread_pool,
                    );
                    server.start(ad.as_str()).unwrap();
                });

                // wait server to start
                thread::sleep(Duration::from_secs(1));

                let client = KvClient::new(addr);

                let (end_work_sender, end_work_receiver) = mpsc::channel::<()>();

                for (key, value) in data.clone() {
                    client.set(key, value).unwrap();
                }

                let counter = Arc::new(AtomicU32::new(0));

                let client_thread_pool = Arc::new(client_thread_pool);
                let data = Arc::new(data);
                let client = Arc::new(client);
                let end_work_sender = Arc::new(end_work_sender);
                b.iter(|| {
                    counter.store(0, std::sync::atomic::Ordering::Release);
                    let client_thread_pool = Arc::clone(&client_thread_pool);

                    for i in 0..ENTRIES_NUMBER {
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
                                    println!("err {e}");
                                }
                            }

                            let prev = counter.fetch_add(1, std::sync::atomic::Ordering::AcqRel);

                            if prev + 1 == ENTRIES_NUMBER {
                                end_work_sender.send(()).unwrap();
                            }
                        });
                    }
                    end_work_receiver.recv().unwrap();
                });
            },
        );
    }
    group.finish();
}

fn async_get_bench(c: &mut Criterion) {
    //ConsoleLayer::builder().init();
    let mut group = c.benchmark_group("async get");
    group.sample_size(SAMPLE_SIZE);
    group.measurement_time(Duration::from_secs(MEASUREMENT_TIME_SEC));

    let workers = 8;

    let server_runtime = Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .unwrap();
    group.bench_function(format!("tokio with {workers} workers"), |bench| {
        let port = find_free_port().unwrap();
        let addres = format!("127.0.0.1:{}", port);
        let temp_dir = TempDir::new().unwrap();

        let client_thread_pool = ThreadPool::new(CLINET_THREADS_NUMBER).unwrap();
        let data = (0..ENTRIES_NUMBER)
            .map(|i| (format!("key{}", i), "value".to_string()))
            .collect::<Vec<(String, String)>>();

        let address = addres.clone();

        server_runtime.block_on(async {
            tokio::spawn(async move {
                let mut server = KvServerAsync::<KvStoreAsync>::new(
                    KvStoreAsync::open(temp_dir.path()).await.unwrap(),
                );
                server.start(address.as_str()).await.unwrap();
            });
        });

        // wait server to start
        thread::sleep(Duration::from_secs(1));

        let client = KvClient::new(addres);

        let (end_work_sender, end_work_receiver) = mpsc::channel::<()>();

        for (key, value) in data.clone() {
            client.set(key, value).unwrap();
        }

        let counter = Arc::new(AtomicU32::new(0));

        let client_thread_pool = Arc::new(client_thread_pool);
        let data = Arc::new(data);
        let client = Arc::new(client);
        let end_work_sender = Arc::new(end_work_sender);
        let client_thread_pool = Arc::clone(&client_thread_pool);
        bench.iter(|| {
            counter.store(0, std::sync::atomic::Ordering::Release);

            for i in 0..ENTRIES_NUMBER {
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
                            println!("requesting {key} err {e}");
                        }
                    }

                    let prev = counter.fetch_add(1, std::sync::atomic::Ordering::Acquire);

                    if prev + 1 == ENTRIES_NUMBER {
                        end_work_sender.send(()).unwrap();
                    }
                });
            }
            end_work_receiver.recv().unwrap();
        });
    });

    group.finish();
}

criterion_group!(benches, async_get_bench, parallel_get_bench);
criterion_main!(benches);
