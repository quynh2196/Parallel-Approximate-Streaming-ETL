// ss.cpp
// Compile:
//   g++ -O3 -fopenmp ss.cpp -o ss
// Run:
//   chcp 65001
//   ./ss.exe

#include <bits/stdc++.h>
#include <chrono>
#include <omp.h>
using namespace std;

#include "ttss.cpp"

/* ========= JSON extractor: mỗi dòng = 1 object ========= */

static bool extract_record_from_line(
    const string &line,
    unordered_map<string,string> &out
) {
    out.clear();

    auto find_field = [&](const string &key)->string {
        string pat = "\"" + key + "\"";
        size_t p = line.find(pat);
        if (p == string::npos) return "";
        size_t colon = line.find(':', p + pat.size());
        if (colon == string::npos) return "";
        size_t q1 = line.find('"', colon + 1);
        if (q1 == string::npos) return "";
        size_t q2 = q1 + 1;
        while (true) {
            q2 = line.find('"', q2);
            if (q2 == string::npos) return "";
            if (line[q2-1] == '\\') { q2++; continue; }
            break;
        }
        return line.substr(q1+1, q2-q1-1);
    };

    string title = find_field("title");
    if (title.empty()) return false;

    out["title"] = title;
    out["topic"] = find_field("topic");
    out["crawled_at"] = find_field("crawled_at");
    return true;
}

/* ========= Reservoir Sampling (ONLINE) ========= */

struct Reservoir {
    size_t k;
    uint64_t seen = 0;
    vector<unordered_map<string,string>> buf;
    mt19937_64 rng{random_device{}()};

    Reservoir(size_t k_) : k(k_) {
        buf.reserve(k);
    }

    void push(const unordered_map<string,string> &x) {
        seen++;
        if (buf.size() < k) {
            buf.push_back(x);
            return;
        }
        uniform_int_distribution<uint64_t> dist(0, seen - 1);
        uint64_t r = dist(rng);
        if (r < k) buf[r] = x;
    }
};

/* ========= Split helper ========= */

static vector<pair<size_t,size_t>>
split_ranges(size_t n, int parts) {
    vector<pair<size_t,size_t>> r(parts);
    for (int i = 0; i < parts; i++) {
        r[i].first  = (n * i) / parts;
        r[i].second = (n * (i + 1)) / parts;
    }
    return r;
}

/* =================== MAIN =================== */

int main() {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    string json_file =
        "C:\\Users\\Admin\\Documents\\ttss\\ttss2\\filtered_news_dataset_7M.json";

    const size_t SAMPLE_SIZE = 7'000'000;
    vector<int> THREAD_LIST = {1,2,4,8,12,16,20};

    ifstream fin(json_file);
    if (!fin.is_open()) {
        cerr << "Cannot open dataset\n";
        return 1;
    }

    /* ========= STREAMING + RESERVOIR ========= */

    Reservoir reservoir(SAMPLE_SIZE);
    unordered_map<string,string> rec;
    string line;
    size_t streamed = 0;

    auto t0 = chrono::high_resolution_clock::now();

    while (getline(fin, line)) {
        if (!extract_record_from_line(line, rec)) continue;
        reservoir.push(rec);
        streamed++;
    }

    auto t1 = chrono::high_resolution_clock::now();

    cout << "Streamed records: " << streamed << "\n";
    cout << "Sample size: " << reservoir.buf.size() << "\n";
    cout << "Streaming time: "
         << chrono::duration<double>(t1 - t0).count()
         << " sec\n\n";

    auto &sample = reservoir.buf;
    size_t n = sample.size();
    double base_time = -1.0;

    /* ========= PARALLEL CMS ========= */

    for (int num_threads : THREAD_LIST) {

        auto ranges = split_ranges(n, num_threads);
        vector<vector<unordered_map<string,string>>> chunks(num_threads);

        for (int i = 0; i < num_threads; i++) {
            auto [s, e] = ranges[i];
            if (s < e)
                chunks[i].assign(sample.begin()+s, sample.begin()+e);
        }

        vector<CountMinSketch> cms_list(num_threads);

        omp_set_num_threads(num_threads);
        auto p0 = chrono::high_resolution_clock::now();

        #pragma omp parallel for
        for (int t = 0; t < num_threads; t++) {
            if (!chunks[t].empty())
                cms_list[t] = streaming_pipeline_realtime(chunks[t]);
        }

        auto p1 = chrono::high_resolution_clock::now();
        double cur_time =
            chrono::duration<double>(p1 - p0).count();

        if (num_threads == 1)
            base_time = cur_time;

        /* ===== MERGE + QUERY ===== */

        CountMinSketch merged = merge_multiple_cms(cms_list);
        string best_title = find_title(sample, cms_list);
        int best_score = compute_title_score(merged, best_title);

        cout << "Threads = " << num_threads
             << " | Time = " << cur_time << " sec";

        if (base_time > 0)
            cout << " | Speedup = " << base_time / cur_time << "x";

        cout << "\nBest title: " << best_title
             << "\nScore: " << best_score
             << "\n--------------------------------\n";
    }

    return 0;
}
