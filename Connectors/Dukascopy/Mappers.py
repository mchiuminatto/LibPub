class BarSizeMapper:
    MAPPER_TO_INT = {"ONE_PIP":1,
               "FIVE_PIPS":5,
               "10_PIPS":10}


    def to_int(self, bar_size) -> int:

        try:
            return self.MAPPER_TO_INT[bar_size]
        except KeyError:
            try:
                return int(bar_size)
            except ValueError:
                raise Exception("No mapping was found and conversion to int was not possible for bar size " + bar_size)
        except Exception as e:
            raise Exception("Error mapping bar size " + bar_size)