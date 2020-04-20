const {BLACK, RED, TOTAL_MEMTABLE_NODES_ACCEPTABLE} = require('./config');

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

class RedBlackTree {
  constructor() {
    this.tree = new Tree();
    this.treeNodesCount = 0;
  }

  insert(key, value) {
    this._insert(new Node(key, value));
    this.treeNodesCount++;
  }

  isAvailable() {
    return this.treeNodesCount < TOTAL_MEMTABLE_NODES_ACCEPTABLE;
  }

  _insert(z) {
    let y = this.tree.nil;
    let x = this.tree.root;

    while (x !== this.tree.nil) {
      y = x;
      if (z.key < x.key) {
        x = x.left
      } else {
        x = x.right;
      }
    }

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

  getValues(node, arr) {
    if (node !== this.tree.nil) {
      this.getValues(node.left, arr);
      arr.push(node);
      this.getValues(node.right, arr);
    }
  }

  reset() {
    this.tree = new Tree();
    this.treeNodesCount = 0;
  }
}

module.exports = RedBlackTree;