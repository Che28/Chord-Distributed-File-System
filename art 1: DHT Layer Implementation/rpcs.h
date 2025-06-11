#ifndef RPCS_H
#define RPCS_H

#include "chord.h"
#include "rpc/client.h"
#include <iostream>
#include <vector>
#include <cmath>
#include <exception>
#include <thread>
#include <unistd.h>

#define m 4 // 指標表的大小，根據需要可調整
#define r 4 // 成功者列表的大小，根據需要可調整

int finger_index = -1;
int next_indedx = -1;

Node self, successor, predecessor;
std::vector<Node> finger_table(m, self); // 指標表
std::vector<Node> successor_list(r, self); // 成功者列表


Node get_info() { return self; } 

Node get_successor() { return successor; }

Node get_predecessor() { return predecessor; }

Node find_successor(uint64_t id); 


void create() {
    predecessor = Node{"", 0, 0}; // 初始化predecessor
    successor = self; // 設定自己為successor
}

void join(Node n) {
    predecessor = Node{"", 0, 0}; // 初始化predecessor
    rpc::client client(n.ip, n.port);
    successor = client.call("find_successor", self.id).as<Node>();
}

bool between(uint64_t id, uint64_t a, uint64_t b) {
    return (a == b) || (a < b ? (id > a && id <= b) : (id > a || id <= b));
}

void stabilize() {
    if (successor.ip.empty()) return;

    try {
        rpc::client client(successor.ip, successor.port);
        Node x = client.call("get_predecessor").as<Node>();

        if (!x.ip.empty() && between(x.id, self.id, successor.id)) {
            successor = x;
        }

        rpc::client(successor.ip, successor.port).call("notify", self);

    } catch (const std::exception &e) {
        bool successor_found = false;

        for (int i = 0; i < r && !successor_found; ++i) {
            try {
                if (!successor_list[i].ip.empty()) {
                    rpc::client backup_client(successor_list[i].ip, successor_list[i].port);
                    backup_client.call("get_info").as<Node>();  // 確認節點可用性
                    successor = successor_list[i];
                    successor_found = true;
                }
            } catch (const std::exception &e) {
                // successor_list[i] 不可用，繼續嘗試下一個
            }
        }

        if (!successor_found) {
            successor = self;  // 若 successor_list 中無可用節點，則重置 successor 為自己
        }
    }
}

void notify(Node n) {
    if (predecessor.ip.empty() || between(n.id, predecessor.id, self.id)) {
        predecessor = n; // 更新 predecessor
    }
}

void fix_fingers() {
    if (!successor.ip.empty() && !predecessor.ip.empty()) {
        finger_index = (finger_index + 1) % m;

        uint64_t start = (self.id + (1ULL << (24 + finger_index))) % (1ULL << 32);
        finger_table[finger_index] = find_successor(start);
    }
}

void check_predecessor() {
    if (!predecessor.ip.empty()) {
        try {
            rpc::client(predecessor.ip, predecessor.port).call("get_info"); // 確認 predecessor 可用
        } catch (const std::exception&) {
            predecessor = Node{"", 0, 0}; // 如果 predecessor 不可用，清空
        }
    }
}

Node closest_preceding_node(uint64_t id) {
    for (int i = m - 1; i >= 0; --i) {
        if (!finger_table[i].ip.empty() && between(finger_table[i].id, self.id, id)) {
            return finger_table[i];
        }
    }
    return self; // 若無匹配則返回自己
}

Node find_successor(uint64_t id) {
    if (id == successor.id || between(id, self.id, successor.id)) {
        return successor; // 若 ID 為 successor ID 或在範圍內，返回 successor
    }

    Node n = closest_preceding_node(id); // 找到最近的 preceding_node
    return (n.id == self.id) ? successor : rpc::client(n.ip, n.port).call("find_successor", id).as<Node>(); // 遞歸調用找到 successor
}

void fix_successor() {
    if (successor.ip.empty()) return;

    try {
        next_indedx = (next_indedx + 1) % r;  // 更新索引並確保範圍內循環

        if (next_indedx == 0) {
            successor_list[next_indedx] = successor;  // 將 successor 設為第一個節點
            return;
        }

        Node next_node = successor_list[next_indedx - 1];  // 取得前一個節點作為 next_node

        try {
            rpc::client client(next_node.ip, next_node.port);

            // 若 next_node 是自己，保持 successor 為目前節點的 successor
            successor_list[next_indedx] = (next_node.id == self.id) 
                ? successor 
                : client.call("get_successor").as<Node>();
            
        } catch (const std::exception&) {
            // 移動 successor_list 中的節點，避免空位
            for (int k = next_indedx; k < r - 1; ++k) {
                successor_list[k] = successor_list[k + 1];
            }
            next_indedx = -1;  // 重設 next_indedx，下次從索引 0 開始
        }
        
    } catch (const std::exception&) {
        // 未預期的異常處理
    }
}


void register_rpcs() {
    add_rpc("get_info", &get_info); 
    add_rpc("get_successor", &get_successor);
    add_rpc("get_predecessor", &get_predecessor);
    add_rpc("create", &create);
    add_rpc("join", &join);
    add_rpc("find_successor", &find_successor);
    add_rpc("notify", &notify);
}

void register_periodics() {
    add_periodic(check_predecessor);
    add_periodic(stabilize);
    add_periodic(fix_fingers);
    add_periodic(fix_successor);
}

#endif /* RPCS_H */

