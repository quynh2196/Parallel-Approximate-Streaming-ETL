#include <bits/stdc++.h>
using namespace std;

uint32_t murmur_hash3(const string& key, uint32_t seed) {
    const uint8_t* data = (const uint8_t*) key.data();
    int len = key.size();
    const int nblocks = len / 4;

    uint32_t h1 = seed;
    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;

    const uint32_t* blocks = (const uint32_t*)(data + nblocks * 4);

    for (int i = -nblocks; i; i++) {
        uint32_t k1 = blocks[i];

        k1 *= c1;
        k1 = (k1 << 15) | (k1 >> 17);
        k1 *= c2;

        h1 ^= k1;
        h1 = (h1 << 13) | (h1 >> 19);
        h1 = h1 * 5 + 0xe6546b64;
    }

    const uint8_t* tail = (data + nblocks * 4);

    uint32_t k1 = 0;

    switch (len & 3) {
    case 3: k1 ^= tail[2] << 16;
    case 2: k1 ^= tail[1] << 8;
    case 1: k1 ^= tail[0];
            k1 *= c1;
            k1 = (k1 << 15) | (k1 >> 17);
            k1 *= c2;
            h1 ^= k1;
    };

    h1 ^= len;

    // finalization
    h1 ^= h1 >> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >> 16;

    return h1;
}

unordered_set<string> VN_STOPWORDS = {
    "và","trong","với","của","là","được","tại","từ","cho","đến",
    "này","kia","đó","ấy","sẽ","đã","đang","cũng",
    "nhưng","hay","hoặc","nếu","thì","rằng","vì","do","khi",
    "trên","dưới","giữa","sau","trước","nơi","đây",
    "người","ông","bà","anh","chị","họ","chúng","ta","tôi",
    "một","hai","ba","nhiều","ít","các","những",
    "năm","tháng","ngày","hôm"
};

vector<string> clean_and_tokenize(const string& text_input) {
    string text = text_input;
    string result;

    for (char c : text) result.push_back(tolower((unsigned char)c));

    // regex replace: keep letters, digits, spaces
    for (char& c : result) {
        if (!(isalnum((unsigned char)c) || isspace((unsigned char)c)
            || (signed char)c >= -64)) {
            c = ' ';
        }
    }

    stringstream ss(result);
    string word;
    unordered_set<string> unique_tokens;
    vector<string> tokens;

    while (ss >> word) {
        if (word.size() > 1 && VN_STOPWORDS.count(word) == 0) {
            unique_tokens.insert(word);
        }
    }

    for (auto& w : unique_tokens) tokens.push_back(w);
    return tokens;
}

class CountMinSketch {
public:
    int width, depth;
    vector<vector<int>> table;

    CountMinSketch(int width = 2000, int depth = 5)
        : width(width), depth(depth), table(depth, vector<int>(width, 0)) {}

    void add(const string& key, int count = 1) {
        for (int i = 0; i < depth; i++) {
            uint32_t idx = murmur_hash3(key, i) % width;
            table[i][idx] += count;
        }
    }

    int query(const string& key) {
        int mn = INT_MAX;
        for (int i = 0; i < depth; i++) {
            uint32_t idx = murmur_hash3(key, i) % width;
            mn = min(mn, table[i][idx]);
        }
        return mn;
    }
};

int compute_title_score(CountMinSketch& cms, const string& title) {
    auto tokens = clean_and_tokenize(title);
    int score = 0;
    for (auto& w : tokens) {
        score += cms.query(w);
    }
    return score;
}

CountMinSketch merge_multiple_cms(vector<CountMinSketch>& cms_list) {
    CountMinSketch base = cms_list[0];

    for (int k = 1; k < cms_list.size(); k++) {
        CountMinSketch& cms = cms_list[k];
        for (int d = 0; d < base.depth; d++) {
            for (int w = 0; w < base.width; w++) {
                base.table[d][w] += cms.table[d][w];
            }
        }
    }

    return base;
}

CountMinSketch streaming_pipeline_realtime(const vector<unordered_map<string,string>>& stream_data) {
    CountMinSketch cms;

    for (auto& item : stream_data) {
        string title = item.at("title");
        auto tokens = clean_and_tokenize(title);
        for (auto& t : tokens) cms.add(t);
    }

    return cms;
}

string find_title(
    const vector<unordered_map<string,string>>& stream_data,
    vector<CountMinSketch>& cms_list
) {
    CountMinSketch cms_last = merge_multiple_cms(cms_list);

    int best_score = -1;
    string best_title = "";

    for (auto& item : stream_data) {
        string title = item.at("title");
        int score = compute_title_score(cms_last, title);
        if (score > best_score) {
            best_score = score;
            best_title = title;
        }
    }

    return best_title;
}
