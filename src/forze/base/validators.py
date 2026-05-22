class NoneValidator:
    """Validator for None values."""

    @staticmethod
    def exactly_one(*values: object) -> bool:
        """Validate that exactly one of the values is not None."""

        already_found = False

        for v in values:
            if v is not None:
                if already_found:
                    return False

                already_found = True

        return already_found

    # ....................... #

    @staticmethod
    def at_least_one(*values: object) -> bool:
        """Validate that at least one of the values is not None."""

        return any(v is not None for v in values)

    # ....................... #

    @staticmethod
    def all_or_none(*values: object) -> bool:
        """Validate that either all of the values are None or all of the values are not None."""

        if not values:
            return True

        first_is_none = values[0] is None

        return all((v is None) == first_is_none for v in values)

    # ....................... #

    @classmethod
    def one_or_none(cls, *values: object) -> bool:
        """Validate that exactly one of the values is not None or all of the values are None."""

        already_found = False

        for v in values:
            if v is not None:
                if already_found:
                    return False

                already_found = True

        return True
