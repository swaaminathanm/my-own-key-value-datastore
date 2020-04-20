const RedBlackTree = require('../RedBlackTree');

test('test correctness of tree', () => {
  const tree = new RedBlackTree();
  const toInsertKeys = [];
  const N = 1000000;

  for (let i=0;i<N;i++) {
    toInsertKeys.push(Math.floor(Math.random() * N));
  }

  const toInsertKeysSorted = toInsertKeys.sort((a, b) => {
    return a - b;
  });

  toInsertKeys.forEach(key => tree.insert(key, {message: 'sample-value'}));

  let treeValues = [];
  tree.getValues(tree.tree.root, treeValues);

  treeValues = treeValues.map(o => o.key);

  expect(treeValues).toEqual(toInsertKeysSorted);
  expect(tree.isAvailable()).toBe(true);
});