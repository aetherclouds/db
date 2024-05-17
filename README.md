## requires
`gem install rspec` for tests

## the gist
B+ tree stores data ONLY on leaves, unlike B-tree.

1 page = 1 B+-tree node
(1st node in an empty tree is initialized as a root leaf node)

order (n. of cells) of internal nodes >= order of leaf nodes
(because we're just cramming as much as we can, and leaf nodes' cells are bigger than internal nodes', so we fit less)

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