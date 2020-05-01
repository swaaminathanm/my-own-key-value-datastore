const RedBlackTree = require('../../RedBlackTree');
const {ONLY_NUMERIC_KEYS_ACCEPTED} = require('../../errors');

describe('Test RedBlackTree', () => {
  const tree = new RedBlackTree(50);
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

  it('should have inserted all the keys', () => {
    let treeValues = tree.getValues();

    treeValues = treeValues.map(o => o.key);

    expect(treeValues).toEqual(toInsertKeys);
  });

  it('tree should be available to insert', () => {
    expect(tree.canAccept()).toBe(true);
  });

  it('should return value if get is called with valid key', () => {
    expect(tree.get(9)).not.toBeNull();
  });

  it('should return undefined if get is called with invalid key', () => {
    expect(tree.get(90)).toBeUndefined();
  });

  it('should throw error if non-numeric key is inserted', () => {
    try {
      tree.insert("non-numeric-key", "value");
    } catch(err) {
      expect(err.code).toBe(ONLY_NUMERIC_KEYS_ACCEPTED);
      expect(err.error.message).toBe('Only numeric keys are accepted');
    }
  });

  it('should call callback with in-order values', () => {
    const logs = [];
    tree.traverse(log => logs.push(log));

    expect(logs.map(log => log.key)).toEqual(toInsertKeys);
  });

  it('should call callback with null values if tree is empty', () => {
    const mockCallBack = jest.fn();
    const tree = new RedBlackTree(50);

    tree.traverse(mockCallBack);

    expect(mockCallBack).toHaveBeenCalledWith({
      key: null,
      value: null
    })
  });
});