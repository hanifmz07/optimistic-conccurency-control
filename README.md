# Optimistic Conccurency Control
Simulation of Optimistic Conccurency Control (OCC) in Database Transactions. In this program, the protocol used for simulating OCC is validation-based protocol.

## Requirements
- Tested in Python 3.10

## How to run
Type this command in the terminal to run the program:
```sh
python main.py 
```
After that, enter the input file path to the program.

Below is the example of the input file used in the program:
```
R,1,B
R,2,B
R,2,A
R,1,A
V,1
P,1
C,1
V,2
W,2,B
W,2,A
C,2
```
Each line represents an operation that will be executed sequentially. Each component of an operation is separated by a comma (,). The first element of an operation is the type of operation. These are the types of operations that you can use in the input file:
- R: Read
- W: Write
- V: Validate
- P: Print/Display
- C: Commit

The second element is the transaction ID. The third element is the data targeted by the operation (only for read and write operation).

## Authors
| Name                          |   NIM    |
| ----------------------------- | :------: |
| Hanif Muhammad Zhafran        | 13521157 |
| Shidqi Indy Izhari            | 13521097 |
| Manuella Ivana Uli Sianipar   | 13521051 |
| Rava Maulana                  | 13521149 |
