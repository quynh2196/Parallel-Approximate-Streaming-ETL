import json

INPUT_FILE  = "filtered_news_dataset.json"      # 70k
OUTPUT_FILE = "filtered_news_dataset_20M.json"   
TARGET = 20000000


def main():
    # load toàn bộ 70k record
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)   # list[dict]

    print("Original records:", len(data))

    out = []
    i = 0

    while len(out) < TARGET:
        out.append(data[i])
        i += 1
        if i == len(data):
            i = 0   # quay lại đầu → lặp đúng 70k ban đầu

        if len(out) % 100_000 == 0:
            print(f"Generated {len(out)} records")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False)

    print("Done.")
    print("Output file:", OUTPUT_FILE)


if __name__ == "__main__":
    main()
