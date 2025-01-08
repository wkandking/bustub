#include "primer/trie.h"
#include <cassert>
#include <cstddef>
#include <memory>
#include <stack>
#include <string_view>
#include <utility>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  if (root_ == nullptr) {
    return nullptr;
  }

  auto node = root_;
  for (const char c : key) {
    auto child_iter = node->children_.find(c);
    if (child_iter == node->children_.end()) {
      return nullptr;
    }
    node = child_iter->second;
  }

  const auto val = dynamic_cast<const TrieNodeWithValue<T> *>(node.get());
  if (val == nullptr) {
    return nullptr;
  }

  assert(val->is_value_node_);
  return val->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  std::shared_ptr<TrieNode> n_root{nullptr};
  if (root_ == nullptr) {
    n_root = std::make_shared<TrieNode>();
  } else {
    n_root = root_->Clone();
  }

  auto node = n_root;
  const size_t bound = key.size() - 1;

  for (size_t idx = 0; idx < key.size(); ++idx) {
    const char c = key[idx];

    if (node->children_.find(c) == node->children_.end()) {
      if (idx == bound) {
        node->children_[c] = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
        return Trie(n_root);
      }
      auto child = std::make_shared<TrieNode>();
      node->children_[c] = child;
      node = child;
      continue;
    }

    auto parent = node;
    // In here, the children[c] can be find
    node = parent->children_[c]->Clone();
    if (idx == bound) {
      parent->children_[c] =
          std::make_shared<TrieNodeWithValue<T>>(node->children_, std::make_shared<T>(std::move(value)));
      return Trie(n_root);
    }
    parent->children_[c] = node;
  }
  assert(key.empty());
  if (root_ == nullptr) {
    n_root = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
  } else {
    n_root = std::make_shared<TrieNodeWithValue<T>>(root_->children_, std::make_shared<T>(std::move(value)));
  }
  return Trie(n_root);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  if (root_ == nullptr) {
    return *this;
  }

  std::shared_ptr<TrieNode> n_root = root_->Clone();
  auto node = n_root;
  if (key.empty()) {
    if (node->is_value_node_) {
      if (node->children_.empty()) {
        // root is the find node, and no children
        return {};
      }
      // root is the find node, and has children
      return Trie(std::make_shared<TrieNode>(std::move(node->children_)));
    }
    // we not find the key
    return *this;
  }

  std::stack<std::pair<std::shared_ptr<TrieNode>, const char>> track;
  size_t bound = key.size() - 1;

  for (size_t idx = 0; idx < key.size(); ++idx) {
    const char c = key[idx];

    auto parent = node;
    auto iter = parent->children_.find(c);
    // we not find the key
    if (iter == parent->children_.end()) {
      return *this;
    }
    if (idx == bound) {
      if (!iter->second->is_value_node_) {
        // we find the key, but the valueNode is not exist
        return *this;
      }
      // we find the valueNode, change the valueNode to TrieNode
      node = std::make_shared<TrieNode>(iter->second->children_);
    } else {
      node = iter->second->Clone();
    }
    parent->children_[c] = node;
    track.push(std::make_pair(parent, c));
  }

  while (!track.empty()) {
    auto parent_pair = track.top();
    auto child = parent_pair.first->children_[parent_pair.second];
    // we can remove the node if the node is not the value node, and no children
    if (child->children_.empty() && !child->is_value_node_) {
      parent_pair.first->children_.erase(parent_pair.second);
    }
    track.pop();
  }

  if (n_root->children_.empty()) {
    return {};
  }
  return Trie(n_root);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
