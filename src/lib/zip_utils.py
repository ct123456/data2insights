STANDARD_ZIP5_LENGTH = 5
STANDARD_ZIP5_PLUS4_LENGTH = 9


def get_zip5(zipcode_string):
    default_zip5 = ""
    if zipcode_string is not None:
        zipcode_string = zipcode_string.encode("utf-8").strip()

    if not isinstance(zipcode_string, str):
        return default_zip5

    if not zipcode_string.isdigit():
        return default_zip5

    if len(zipcode_string) < STANDARD_ZIP5_LENGTH:
        return add_leading_zeros_to_zipcode(zipcode_string)


def get_plus4(zipcode_string):
    default_plus4 = ""

    if not isinstance(zipcode_string, str):
        return default_plus4

    if not zipcode_string.isdigit():
        return default_plus4

    if len(zipcode_string) < STANDARD_ZIP5_LENGTH:
        return default_plus4

    if len(zipcode_string) != STANDARD_ZIP5_PLUS4_LENGTH:
        return default_plus4

    return zipcode_string[STANDARD_ZIP5_LENGTH:]


def add_leading_zeros_to_zipcode(zipcode_string):

    if len(zipcode_string) >= STANDARD_ZIP5_LENGTH:
        return zipcode_string

    return zipcode_string.zfill(STANDARD_ZIP5_LENGTH)
