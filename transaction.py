class Transaction:
    def __init__(
        self,
        id,
        read_set,
        write_set,
    ):
        self.id = id
        self.read_set = read_set
        self.write_set = write_set
        self.start_ts = 0
        self.validation_ts = 0
        self.finish_ts = 0
        self.operations = []

    # The updated start timestamp is the finish_ts + 1 of the previous transaction
    def update_timestamp_rollback(self, start_ts):
        self.start_ts = start_ts
        for i in range(len(self.operations)):
            if self.operations[i][0] == "V":
                self.validation_ts = self.start_ts + i
                break
        self.finish_ts = self.start_ts + len(self.operations) - 1

    # Used when another transaction is rolled back
    def decrement_timestamp(self):
        self.start_ts -= 1
        self.validation_ts -= 1
        self.finish_ts -= 1

    def __str__(self) -> str:
        return f"Transaction {self.id} with: \nRead set: {self.read_set} \nWrite set: {self.write_set} \nStarted timestamp: {self.start_ts} \nValidate timestamp: {self.validation_ts} \nFinish timestamp: {self.finish_ts}"
