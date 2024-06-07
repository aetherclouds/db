## requires
`gem install rspec` for tests

## the gist
B+ trees stores data ONLY on leaves, unlike B-trees.
1 page = 1 B+-tree node
(1st node in an empty tree is initialized as a root leaf node)
internal nodes store children as page indices (rather than e.g. pointers)
a `Pager` manages pages. accessing data should be done through it (`get_page`) so it can handle loading from disk.
a `Cursor` uniquely identifies a page and a cell within it. they are not a singleton and may be instanced 
a `Table` contains a pager and the position of the root node. it does not contain a schema as that is both global (memory offsets) and described by `Row` (this is probably prone to change if this database is ever actually used)

order (n. of cells) of internal nodes >= order of leaf nodes
(because we're just cramming as much as we can, and leaf nodes' cells are bigger than internal nodes', so we have to fit less)

## usage
```
meta commands:
- .exit
- .btree # print data tree structure
- .print # print constants

commands:
- insert %field1% %field2% %fieldn%
- select
```