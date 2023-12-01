from transaction import Transaction


class Schedule:
    def __init__(self):
        # Read input file
        filename = input("Input filename: ")
        with open(filename, "r") as f:
            data = f.read()
        parse_command = lambda x: tuple(x.split(sep=","))

        # List of operations
        # Each operation is a tuple of (operation, transaction_id, data)
        self.operations = list(map(parse_command, data.split(sep="\n")))
        self.transaction_set = set()
        self.transaction_map = {}

    # Initiate transaction map
    def __generate_transaction_map(self):
        # Generate transaction id set
        for operation in self.operations:
            self.transaction_set.add(operation[1])

        # Generate transaction map with empty read and write set
        for id_transaction in list(self.transaction_set):
            self.transaction_map[id_transaction] = Transaction(
                id_transaction, set(), set()
            )

        # Generate read and write set for each transaction and its timestamp
        timestamp = 1
        for operation in self.operations:
            # Add the operation to the read set of the transaction
            if operation[0] == "R":
                # If the transaction has not read or written anything yet, then it is the start timestamp
                if (not self.transaction_map[operation[1]].read_set) and (
                    not self.transaction_map[operation[1]].write_set
                ):
                    self.transaction_map[operation[1]].start_ts = timestamp
                self.transaction_map[operation[1]].read_set.add(operation[2])

            # Add the operation to the write set of the transaction
            elif operation[0] == "W":
                if (not self.transaction_map[operation[1]].read_set) and (
                    not self.transaction_map[operation[1]].write_set
                ):
                    self.transaction_map[operation[1]].start_ts = timestamp
                self.transaction_map[operation[1]].write_set.add(operation[2])

            # Set the validation timestamp of the transaction
            elif operation[0] == "V":
                self.transaction_map[operation[1]].validation_ts = timestamp

            # Last operation timestamp is the finish timestamp of a transaction
            self.transaction_map[operation[1]].finish_ts = timestamp

            # Append the operation to the transaction operations attribute
            self.transaction_map[operation[1]].operations.append(operation)

            timestamp += 1

            # Sort the transaction map by validation timestamp
            self.transaction_map = dict(
                sorted(self.transaction_map.items(), key=lambda x: x[1].validation_ts)
            )

    # Check if the transaction is valid
    def __validation_test(self, id):
        # Generate copy of transaction map without the transaction being validated
        transaction_map_check = self.transaction_map.copy()
        transaction_map_check.pop(id)

        # Check if the transaction is valid
        for _, transaction_check in transaction_map_check.items():
            if (
                self.transaction_map[id].validation_ts
                <= transaction_check.validation_ts
            ):
                valid = True
                if (
                    transaction_check.start_ts < self.transaction_map[id].finish_ts
                    and self.transaction_map[id].finish_ts
                    < transaction_check.validation_ts
                ):
                    if self.transaction_map[id].write_set.intersection(
                        transaction_check.read_set
                    ):
                        print(
                            f"Transaction {id} writes data that is read by transaction {transaction_check.id}"
                        )
                        valid = False
                elif self.transaction_map[id].finish_ts >= transaction_check.start_ts:
                    print(
                        f"Finish timestamp of transaction {id} is greater than start timestamp of transaction {transaction_check.id}"
                    )
                    valid = False

                if not valid:
                    print(f"Transaction {id} is invalid")
                    print(
                        f"Transaction {id} will be aborted and restarted after the last transaction"
                    )
                    return False
                else:
                    print(f"Transaction {id} is valid")
                    return True
        print(f"Transaction {id} is valid")
        return True

    # Run schedule with OCC
    def run_schedule(self):
        self.__generate_transaction_map()
        timestamp = 1

        while self.operations:
            # Operation that is being processed is the first element of the list
            operation = self.operations.pop(0)

            # Read operation
            if operation[0] == "R":
                print(
                    f"Transaction {operation[1]} reads data {operation[2]} at timestamp {timestamp}"
                )
            # Write operation
            elif operation[0] == "W":
                print(
                    f"Transaction {operation[1]} writes data {operation[2]} at timestamp {timestamp}"
                )
            # Validation phase
            elif operation[0] == "V":
                print(
                    f"Validating transaction {operation[1]} at timestamp {self.transaction_map[operation[1]].validation_ts}"
                )
                # If valid, transaction entered write phase and is committed
                if self.__validation_test(operation[1]):
                    self.transaction_map.pop(operation[1])
                # If invalid, transaction is aborted and restarted after the last transaction
                else:
                    last_transaction = self.transaction_map[
                        list(self.transaction_map.keys())[-1]
                    ]
                    invalid_transaction = self.transaction_map.pop(operation[1])
                    while self.operations[0][1] == operation[1]:
                        self.operations.pop(0)
                        for id, transaction in self.transaction_map.items():
                            transaction.decrement_timestamp()
                    self.transaction_map[operation[1]] = invalid_transaction
                    self.operations.extend(
                        self.transaction_map[operation[1]].operations
                    )
                    self.transaction_map[operation[1]].update_timestamp_rollback(
                        last_transaction.finish_ts + 1
                    )
            # Print operation
            elif operation[0] == "P":
                print(
                    f"Transaction {operation[1]} displays data at timestamp {timestamp}"
                )
            # Commit operation
            elif operation[0] == "C":
                print(f"Transaction {operation[1]} commits at timestamp {timestamp}")
            else:
                print("Invalid operation")
                exit()
            timestamp += 1


if __name__ == "__main__":
    schedule = Schedule()
    schedule.run_schedule()
