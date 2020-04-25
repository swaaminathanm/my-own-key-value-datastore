const RedBlackTree = require('../RedBlackTree');

test('test correctness of tree', () => {
  const tree = new RedBlackTree();
  const toInsertKeys = [];
  const N = 10;

  for (let i=0;i<N;i++) {
    toInsertKeys.push(Math.floor(Math.random() * 5));
  }

  toInsertKeys.forEach(key => tree.insert(key, {message: 'sample-value'}));

  toInsertKeys.sort((a, b) => {
    return a - b;
  });
  toInsertKeys.splice(0, toInsertKeys.length, ...(new Set(toInsertKeys)));

  let treeValues = [];
  tree.getValues(tree.tree.root, treeValues);

  treeValues = treeValues.map(o => o.key);

  expect(treeValues).toEqual(toInsertKeys);
  expect(tree.isAvailable()).toBe(true);
});