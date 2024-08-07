class DownloadError(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

class S3Error(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

class TableNotFound(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)