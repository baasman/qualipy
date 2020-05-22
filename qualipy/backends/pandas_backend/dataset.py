from qualipy.backends.base import BaseData


class PandasData(BaseData):
    pass

    def set_fallback_data(self):
        return self.data.head(0)
