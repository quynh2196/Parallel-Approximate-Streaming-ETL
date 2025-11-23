import time
import multiprocessing as mp
from crawl_data import main
from ttss import (CountMinSketch, clean_and_tokenize, merge_multiple_cms, compute_title_score, find_title)

def worker(input_queue, output_queue):
    cms = CountMinSketch()
    num_processed = 0 # so luong bai bao
    while True:
        item = input_queue.get()
        if item == "DONE":
            break
        title = item["title"]
        for t in clean_and_tokenize(title):
            cms.add(t)
        num_processed += 1
    output_queue.put((cms, num_processed))

if __name__ == "__main__":
    # lay data trong run_time seconds --> lay cung 1 data cho dong deu
    run_time = 10  # seconds
    stream = main()
    fixed_data = []
    start = time.time()
    while time.time() - start < run_time:
        fixed_data.append(next(stream))
    print(f"length = {len(fixed_data)}")

    for n_workers in [1, 2, 4, 8]:
        print(f"\n {n_workers} workers")
        input_queue = mp.Queue()
        output_queue = mp.Queue()
        procs = [
            mp.Process(target=worker, args=(input_queue, output_queue))
            for _ in range(n_workers)
        ]

        for p in procs:
            p.start()

        for item in fixed_data:
            input_queue.put(item) # day toan bo data vao input_queue

        for _ in range(n_workers):
            input_queue.put("DONE")

        start_time = time.time()
        cms_list = []
        total_processed = 0
        for _ in range(n_workers):
            cms_part, cnt = output_queue.get()
            cms_list.append(cms_part)
            total_processed += cnt

        final_cms = merge_multiple_cms(cms_list)
        best_title = find_title(fixed_data, cms_list)
        best_score = compute_title_score(final_cms, best_title)

        for p in procs:
            p.join()
        end_time = time.time()

        print(f"Processed: {total_processed} articles")
        print(f"Best title: {best_title}  (score={best_score})")
        print(f"Time: {(end_time - start_time)*1000:.2f} ms")

