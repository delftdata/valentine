//============================================================================
// Name        : GraphIndex.cpp
// Author      : Raul Castro Fernandez
// Version     : 0.1
// Copyright   : Whatever Aurum's license is
// Description : Graph Index Skeleton
// g++ -dynamiclib -o graph_index.so GraphIndex.cpp
//============================================================================

#include <iostream>
#include <sstream>
#include <iterator>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <thread>
#include <string>
#include <new>  // std::nothrow
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <array>
#include <ctime>
using namespace std;

/***
** Interface
***/


// Global variables
std::unordered_map<int, std::unordered_map<int, char> > g;
int number_edges;

template<typename Out>
void split(const std::string &s, char delim, Out result) {
    std::stringstream ss;
    ss.str(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        *(result++) = item;
    }
}

vector<string> split(string &s, char delim) {
    vector<string> tokens;
    split(s, delim, back_inserter(tokens));
    return tokens;
}

extern "C" {

    int get_num_nodes() {
        return g.size();
    }

    int get_num_edges() {
        return number_edges;
    }

    bool add_node(int id) {
        if (g.count(id) == 1) {
            return false;
        }
        g[id];
        return true;
    }

    bool add_edge(int source_id, int target_id, char type) {
        if (source_id == target_id) {
            return false;
        }
        add_node(source_id);
        add_node(target_id);
        if (g[source_id].count(target_id) == 0) {
            g[source_id][target_id] = type;
            number_edges++;
        }
        else if ((g[source_id][target_id] | type) == 0) {
            g[source_id][target_id] |= type;
            number_edges++;
        }
        return true;
    }

    bool add_undirected_edge(int source_id, int target_id, char type) {
        add_edge(source_id, target_id, type);
        add_edge(target_id, source_id, type);
        return true;
    }

    int neighbors(int32_t** output, int id, char type) {
        vector<int> n;
        unordered_map<int, char> nodes_map = g[id];
        for ( auto it = nodes_map.begin(); it != nodes_map.end(); ++it) {
            if ((it->second & type) == 1) {
                n.push_back(it->first);
            }
        }

        // copy to array
        int32_t* array = (int32_t*) malloc(n.size() * sizeof(int32_t));
        //int array[n.size()];
        for (int i = 0; i < n.size(); i++) {
            int id = n[i];
            cout << id << endl;
            array[i] = id;
        }

        *output = array;
        cout << n.size() << endl;
        return n.size();
    }

    vector<int> neighbors_local(int id, char type) {
        vector<int> n;
        unordered_map<int, char> nodes_map = g[id];
        for ( auto it = nodes_map.begin(); it != nodes_map.end(); ++it) {
            if ((it->second & type) == 1) {
                n.push_back(it->first);
            }
        }
        return n;
    }

    void release_array(int32_t *input) {
        free(input);
    }

    int all_paths(int32_t** output, int source_id, int target_id, char type, int max_hops) {
        //cout << "find path from " + to_string(source_id) + " to: " + to_string(target_id) << endl;
        clock_t begin = clock();
        int level = 0;
        vector<int> next_level;
        next_level.push_back(source_id);
        unordered_map<int, int> seen;
        seen[source_id] = level;
        unordered_map<int, vector<int> > pred;
        pred[source_id];
//        cout << "before while: " + to_string(next_level.size()) << endl;
        while (next_level.size() > 0) {
            level += 1;
            vector<int> this_level = next_level;
//            cout << "before clear: " + to_string(next_level.size()) << endl;
            next_level.clear();
//            cout << "after clear: " + to_string(next_level.size()) << endl;
//            for (int a = 0; a < next_level.size(); a++) {
//                cout << "next_level: " + to_string(a) << endl;
//            }
            //for(auto it = this_level.begin(); it != this_level.end(); ++it) {
            for(int i = 0; i < this_level.size(); i++) {
                int el = this_level[i];
                vector<int> neighbors = neighbors_local(el, type);
                //for (auto it2 = neighbors.begin(); it2 != neighbors.end(); ++it2) {
                for(int j = 0; j < neighbors.size(); j++) {
                    int n = neighbors[j];
                    if (seen.count(n) == 0) {
                        vector<int> els;
                        els.push_back(el);
                        pred[n] = els;
                        seen[n] = level;
                        next_level.push_back(n);
                    }
                    else if (seen[n] == level) {
                        pred[n].push_back(el);
                    }
                }
            }
            if (max_hops <= level){
                break;
            }
        }
        if(pred.count(target_id) == 0) {
            return 0;
        }
        vector<array<int, 2> > stack;
        array<int, 2> tuple = {target_id, 0};
        stack.push_back(tuple);
        int top = 0;
        vector<vector<int> > results;
        while (top >= 0) {
            array<int, 2> t = stack[top];
            int node = t[0];
            int i = t[1];
            if(node == source_id) {
                vector<int> result;
                for(int k = 0; k < top + 1; k++) {
                    int j = (top) - k;
                    array<int, 2> jt = stack[j];
                    int el = jt[0];
                    result.push_back(el);
                }
                results.push_back(result);
            }
            if(pred[node].size() > i) {
                top += 1;
                array<int, 2> new_tuple = {pred[node][i], 0};
                if(top == stack.size()) {
                    stack.push_back(new_tuple);
                }
                else {
                    stack[top] = new_tuple;
                }
            }
            else {
                stack[(top - 1)][1] += 1;
                top -= 1;
            }
        }

        //cout << "Num found paths: " + to_string(results.size()) << endl;
        int total_paths_plus_nodes = 0;
        for(int i = 0; i < results.size(); i++) {
            total_paths_plus_nodes += 1;
            vector<int> result = results[i];
            //cout << "---" << endl;
            for (int j =0; j < result.size(); j++) {
                total_paths_plus_nodes += 1;
                //cout << to_string(result[j]) << endl;
            }
        }

        // copy to array
        int32_t* array = (int32_t*) malloc(total_paths_plus_nodes * sizeof(int32_t));
        int ptr = -1;
        //int array[n.size()];
        for(int i = 0; i < results.size(); i++) {
            ptr += 1;
            array[ptr] = -1; // Indicating init of path
            vector<int> result = results[i];
            for (int j =0; j < result.size(); j++) {
                ptr += 1;
                array[ptr] = result[j];
            }
        }

        clock_t end = clock();
        double secs = double(end - begin) / CLOCKS_PER_SEC;
        cout << secs << endl;

        *output = array;
        return total_paths_plus_nodes;
    }

    int internal_all_paths(int source_id, int target_id, char type, int max_hops) {
        cout << "find path from " + to_string(source_id) + " to: " + to_string(target_id) << endl;
        int level = 0;
        vector<int> next_level;
        next_level.push_back(source_id);
        unordered_map<int, int> seen;
        seen[source_id] = level;
        unordered_map<int, vector<int> > pred;
        pred[source_id];
//        cout << "before while: " + to_string(next_level.size()) << endl;
        while (next_level.size() > 0) {
            level += 1;
            vector<int> this_level = next_level;
//            cout << "before clear: " + to_string(next_level.size()) << endl;
            next_level.clear();
//            cout << "after clear: " + to_string(next_level.size()) << endl;
//            for (int a = 0; a < next_level.size(); a++) {
//                cout << "next_level: " + to_string(a) << endl;
//            }
            //for(auto it = this_level.begin(); it != this_level.end(); ++it) {
            for(int i = 0; i < this_level.size(); i++) {
                int el = this_level[i];
                vector<int> neighbors = neighbors_local(el, type);
                //for (auto it2 = neighbors.begin(); it2 != neighbors.end(); ++it2) {
                for(int j = 0; j < neighbors.size(); j++) {
                    int n = neighbors[j];
                    if (seen.count(n) == 0) {
                        vector<int> els;
                        els.push_back(el);
                        pred[n] = els;
                        seen[n] = level;
                        next_level.push_back(n);
                    }
                    else if (seen[n] == level) {
                        pred[n].push_back(el);
                    }
                }
            }
            if (max_hops <= level){
                break;
            }
        }
        if(pred.count(target_id) == 0) {
            return 0;
        }
        vector<array<int, 2> > stack;
        array<int, 2> tuple = {target_id, 0};
        stack.push_back(tuple);
        int top = 0;
        vector<vector<int> > results;
        while (top >= 0) {
            array<int, 2> t = stack[top];
            int node = t[0];
            int i = t[1];
            if(node == source_id) {
                vector<int> result;
                for(int k = 0; k < top + 1; k++) {
                    int j = (top) - k;
                    array<int, 2> jt = stack[j];
                    int el = jt[0];
                    result.push_back(el);
                }
                results.push_back(result);
            }
            if(pred[node].size() > i) {
                top += 1;
                array<int, 2> new_tuple = {pred[node][i], 0};
                if(top == stack.size()) {
                    stack.push_back(new_tuple);
                }
                else {
                    stack[top] = new_tuple;
                }
            }
            else {
                stack[(top - 1)][1] += 1;
                top -= 1;
            }
        }

        cout << "Num found paths: " + to_string(results.size()) << endl;
        int total_paths_plus_nodes = 0;
        for(int i = 0; i < results.size(); i++) {
            total_paths_plus_nodes += 1;
            vector<int> result = results[i];
            cout << "---" << endl;
            for (int j =0; j < result.size(); j++) {
                total_paths_plus_nodes += 1;
                cout << to_string(result[j]) << endl;
            }
        }
        return 1;
    }

    void serialize_graph_to_disk(char* input_path) {
        string path = input_path;
        cout << "Serializing graph to: " + path << endl;
        ofstream f;
        f.open(path);
        for ( auto it = g.begin(); it != g.end(); ++it) {
            int src = it->first;
            unordered_map<int, char> sub = it->second;
            for ( auto it2 = sub.begin(); it2 != sub.end(); ++it2) {
                int tgt = it2->first;
                char type = it2->second;
                string ser = std::to_string(src) + "-" + std::to_string(tgt) + "-" + std::to_string(type) + "\n";
                cout << ser << endl;
                f << ser;
            }
        }
        f.close();
    }

    void deserialize_graph(char* input_path) {
        string path = input_path;
        cout << "Deserializing graph to: " + path << endl;
        string line;
        ifstream f(path);
        bool first = true;
        if (f.is_open()) {
            while (getline(f, line)) {
                if(first) {
                    first = false;
                    continue;
                }
                vector<string> tokens = split(line, ',');
                string src = tokens[0];
                string tgt = tokens[1];
                string type = tokens[2];
//                cout << src + " . " + tgt + " . " + type << '\n';
                // TODO: transform to int and char first
                int src_id = stoi(src);
                int tgt_id = stoi(tgt);
                //char type_ch = (char)(stoi(type));
                char type_ch = (char)(stoi("1"));
                add_node(src_id);
                add_node(tgt_id);
                add_edge(src_id, tgt_id, type_ch); // FIXME: pass right type
            }
        }
        f.close();
    }

}


/***
** OO Interface
***/

class GraphIndex {
public:

    int test_add_one(int i) {
        return i + 1;
    }

    int get_num_nodes() {
        return g.size();
    }

    int get_num_edges() {
        return number_edges;
    }

    bool add_node(int id) {
        if (g.count(id) == 1) {
            return false;
        }
        g[id];
        return true;
    }

    bool add_edge(int source_id, int target_id, char type) {
        if (source_id == target_id) {
            return false;
        }
        this->add_node(source_id);
        this->add_node(target_id);
        if (g[source_id].count(target_id) == 0) {
            g[source_id][target_id] = type;
            number_edges++;
        }
        else if ((g[source_id][target_id] | type) == 0) {
            g[source_id][target_id] |= type;
            number_edges++;
        }
        return true;
    }

    bool add_undirected_edge(int source_id, int target_id, char type) {
        this->add_edge(source_id, target_id, type);
        this->add_edge(target_id, source_id, type);
        return true;
    }

    vector<int> neighbors(int id, char type) {
        vector<int> n;
        unordered_map<int, char> nodes_map = g[id];
        for ( auto it = nodes_map.begin(); it != nodes_map.end(); ++it) {
            if ((it->second & type) == 1) {
                n.push_back(it->first);
            }
        }
        return n;
    }

private:
    std::unordered_map<int, std::unordered_map<int, char> > g;
    int number_edges;
};

/**
** Some basic testing
**/

// Testing iface with Python

extern "C" void myprint(void);

extern "C" void myprint() {

    printf("hellow world from graph index\n");
}

extern "C" int add_one(int i) {
    return i + 1;
}

int main() {

    string path = "random_graph_test.txt";
     char* p = (char*)path.c_str();
    deserialize_graph(p);

    int nnodes = get_num_nodes();
    int nedges = get_num_edges();
    cout << nnodes << endl;
    cout << nedges << endl;

    internal_all_paths(0, 10, 1, 10);


//    class GraphIndex g;
//
//    g.add_node(0);
//    g.add_node(1);
//    g.add_node(2);
//    g.add_edge(0, 1, 33);
//    int nn = g.get_num_nodes();
//    int e = g.get_num_edges();
//
//    vector<int> neighbors = g.neighbors(0, 33);
//
//    for (auto it = neighbors.begin(); it != neighbors.end(); ++it) {
//        cout << *it << endl;
//    }
//
//    cout << nn << endl;
//    cout << e << endl;
//
//
//    class GraphIndex lite;
//
//    int num_nodes = 10000;
//
//    for (int i = 0; i < num_nodes; i++) {
//        g.add_node(i);
//    }
//
//    for (int i = 0; i < num_nodes; i++) {
//        for (int j = 0; j < num_nodes; j++) {
//            g.add_edge(i, j, 1);
//        }
//    }
//
//    for (int i = 0; i < num_nodes; i++) {
//        for (int j = 0; j < num_nodes; j++) {
//            g.add_edge(i, j, 2);
//        }
//    }
//
//    int numnodes = g.get_num_nodes();
//    int numedges = g.get_num_edges();
//
//    cout << numnodes << endl;
//    cout << numedges << endl;
//
//    std::this_thread::sleep_for(std::chrono::seconds(10));
//    cout << "done" << endl;

}

