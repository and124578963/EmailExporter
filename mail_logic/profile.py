from typing import List


class ConfigProfile:
    def __init__(self, **kwargs):
        self.login = kwargs["source"]["login"]
        self.passw = kwargs["source"]["password"]
        self.folder = kwargs["source"]["folder"]
        self.imap_host = kwargs["source"]["imap_host"]

        image = kwargs.get("image", {})
        self.force_to_image = image.get("force_to_image", False)
        self.max_width_px = image.get("max_width_px", 800)
        self.max_height_px = image.get("max_height_px", 1400)

        self.last_row_of_letter = kwargs.get("regex_last_string_mask", "")
        self.replacements: List[dict] = kwargs.get("replacements", [])
        self.ext_fields = kwargs.get("extra_fields", {})
        self.receiver_regex_mask = kwargs["filters"].get("receiver_regex_mask", ".*")
        self.restricted_subjects_regex = kwargs["filters"]["restricted_subjects_regex"]
