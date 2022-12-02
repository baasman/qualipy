import typing as t


class CustomReturn:
    def __init__(self) -> None:
        self.metrics = []

    def add_value(
        self,
        value,
        run_name: str = None,
        metric_name: str = None,
        arguments: str = None,
        meta: t.Dict[str, t.Any] = None,
    ):
        result = dict(
            value=value,
            metric=metric_name,
            run_name=run_name,
            arguments=arguments,
            meta=meta,
        )

        self.metrics.append(result)

    def to_list(self) -> list:
        return self.metrics
