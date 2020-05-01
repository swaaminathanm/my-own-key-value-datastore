const {ONLY_NUMERIC_KEYS_ACCEPTED} = require('./errors');

const RED = 'red';
const BLACK = 'black';

class Node {
  constructor(key, value, color = RED) {
    this.key = key;
    this.value = value;
    this.color = color;
    this.left = null;
    this.right = null;
    this.parent = null;
  }
}

class Tree {
  constructor() {
    this.nil = new Node(null, null, BLACK);
    this.root = this.nil;
  }
}

/**
 * Criteria:
 * + Only numeric keys are allowed
 */
class RedBlackTree {
  constructor(TOTAL_MEMTABLE_NODES_ACCEPTABLE) {
    this.tree = new Tree();
    this.treeNodesCount = 0;
    this.TOTAL_MEMTABLE_NODES_ACCEPTABLE = TOTAL_MEMTABLE_NODES_ACCEPTABLE;
  }

  isEmpty() {
    return this.treeNodesCount === 0;
  }

  insert(key, value) {
    if (!isNaN(key)) {
      this._insert(new Node(key, value));
      this.treeNodesCount++;
    } else {
      throw {
        code: ONLY_NUMERIC_KEYS_ACCEPTED,
        error: {
          message: 'Only numeric keys are accepted'
        }
      };
    }
  }

  get(key) {
    let x = this.tree.root;
    let value = null;

    while (x !== this.tree.nil) {
      if (key < x.key) {
        x = x.left
      } else if (key === x.key) {
        value = x.value;
        break;
      } else {
        x = x.right;
      }
    }

    return value;
  }

  canAccept() {
    return this.treeNodesCount < this.TOTAL_MEMTABLE_NODES_ACCEPTABLE;
  }

  _insert(z) {
    let y = this.tree.nil;
    let x = this.tree.root;

    while (x !== this.tree.nil) {
      y = x;
      if (z.key < x.key) {
        x = x.left
      } else if (z.key === x.key) {
        break;
      } else {
        x = x.right;
      }
    }

    if (z.key === y.key) {
      // If there is a key already in the tree
      // then update it with the new value
      y.value = z.value;
    } else {
      z.parent = y;

      if (y === this.tree.nil) {
        this.tree.root = z;
      } else if (z.key < y.key) {
        y.left = z;
      } else {
        y.right = z;
      }

      z.left = this.tree.nil;
      z.right = this.tree.nil;
      z.color = RED;

      this._insertFixup(z);
    }
  }

  _insertFixup(z) {
    while (z.parent.color === RED) {
      if (z.parent === z.parent.parent.left) {
        let y = z.parent.parent.right;

        if (y.color === RED) {
          z.parent.color = BLACK;
          y.color = BLACK;
          z.parent.parent.color = RED;
          z = z.parent.parent;
        } else if (z === z.parent.right) {
          z = z.parent;
          this._leftRotate(z);
        } else {
          z.parent.color = BLACK;
          z.parent.parent.color = RED;
          this._rightRotate(z.parent.parent);
        }
      } else {
        let y = z.parent.parent.left;

        if (y.color === RED) {
          z.parent.color = BLACK;
          y.color = BLACK;
          z.parent.parent.color = RED;
          z = z.parent.parent;
        } else if (z === z.parent.left) {
          z = z.parent;
          this._rightRotate(z);
        } else {
          z.parent.color = BLACK;
          z.parent.parent.color = RED;
          this._leftRotate(z.parent.parent);
        }
      }
    }
    this.tree.root.color = BLACK;
  }

  _rightRotate(y) {
    let x = y.left;

    y.left = x.right;

    if (x.right !== this.tree.nil) {
      x.right.parent = y;
    }

    x.parent = y.parent;

    if (y.parent === this.tree.nil) {
      this.tree.root = x;
    } else if (y === y.parent.left) {
      y.parent.left = x;
    } else {
      y.parent.right = x;
    }

    x.right = y;
    y.parent = x;
  }

  _leftRotate(x) {
    let y = x.right;

    x.right = y.left;

    if (y.left !== this.tree.nil) {
      y.left.parent = x;
    }

    y.parent = x.parent;

    if (x.parent === this.tree.nil) {
      this.tree.root = y;
    } else if (x === x.parent.left) {
      x.parent.left = y;
    } else {
      x.parent.right = y;
    }

    y.left = x;
    x.parent = y;
  }

  /**
   * This function does In-order traversal of tree and sends data of each node to callback
   * @param cb
   */
  traverse(cb) {
    const stack = [];
    let current = this.tree.root;

    if (current === this.tree.nil) {
      cb({
        key: null,
        value: null
      });
      return;
    }

    stack.push(current);

    while (stack.length > 0) {
      current = current.left;

      while (current !== this.tree.nil) {
        stack.push(current);
        current = current.left;
      }

      while (stack.length > 0 && current === this.tree.nil) {
        current = stack.pop();

        cb({
          key: current.key,
          value: current.value
        });

        current = current.right;
      }

      if (current !== this.tree.nil) {
        stack.push(current);
      }
    }
  }

  /**
   * This function traverses node using In-order traversal of tree and returns array of all the nodes
   */
  getValues() {
    const arr = [];
    this._getValues(this.tree.root, arr);
    return arr;
  }

  _getValues(node, arr) {
    if (node !== this.tree.nil) {
      this._getValues(node.left, arr);
      arr.push({
        key: node.key,
        value: node.value
      });
      this._getValues(node.right, arr);
    }
  }

  reset() {
    this.tree = new Tree();
    this.treeNodesCount = 0;
  }
}

module.exports = RedBlackTree;