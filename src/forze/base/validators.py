class NoneValidator:
    """Validator for None values."""

    @staticmethod
    def exactly_one(*values: object) -> bool:
        """Validate that exactly one of the values is not None."""

        return sum(1 for v in values if v is not None) == 1

    # ....................... #

    @staticmethod
    def at_least_one(*values: object) -> bool:
        """Validate that at least one of the values is not None."""

        return sum(1 for v in values if v is not None) >= 1

    # ....................... #

    @staticmethod
    def all_or_none(*values: object) -> bool:
        """Validate that either all of the values are None or all of the values are not None."""

        all_none = all(v is None for v in values)
        all_not_none = all(v is not None for v in values)

        return all_none or all_not_none

    # ....................... #

    @classmethod
    def one_or_none(cls, *values: object) -> bool:
        """Validate that exactly one of the values is not None or all of the values are None."""

        exactly_one = cls.exactly_one(*values)
        at_least_one = cls.at_least_one(*values)

        return exactly_one or not at_least_one
