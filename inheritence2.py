from abc import abstractmethod
from enum import Enum
from typing import Optional

import pandas as pd

_DF = pd.DataFrame(
    {
        "user_id": ["user_1", "user_1"],
        "offer_id": ["offer_1", "offer_2"],
        "platform": [1, 1],
        "offer_type": [1, 1],
    }
)


class ModelType(Enum):
    RANDOM_FOREST = "random_forest"
    XGBOOST = "xgboost"
    LIGHTGBM = "lightgbm"


class BaseModel(object):
    # Base class for all personalisedAlternative models.

    def __init__(self, df_infer: pd.DataFrame, model_type: Optional[ModelType] = None):
        self._df_infer = df_infer
        self._model_type = model_type

    @abstractmethod
    def _set_features_in_place(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        set relevant features in the inference dataframe
        :param df: inference data frame
        :returns: pd.DataFrame
        """
        pass

    @abstractmethod
    def get_alternative_recommendations(self) -> pd.DataFrame:
        # Returns alternative recommendations
        pass

    @classmethod
    @abstractmethod
    def get_model_version(cls) -> int:
        """
        Returns current model version that will be presented in response
        """
        pass


class BaseModelV1(BaseModel):
    def __init__(self, df_infer: pd.DataFrame, model_type: Optional[ModelType] = None):
        super().__init__(df_infer, model_type)

    @abstractmethod
    def _set_features_in_place(self, df: pd.DataFrame) -> pd.DataFrame:
        df["model_type"] = self._model_type
        return df


class ModelV0(BaseModelV1):
    def __init__(self, df_infer: pd.DataFrame):
        super().__init__(df_infer=df_infer, model_type=None)

    @abstractmethod
    def get_alternative_recommendations(self) -> pd.DataFrame:
        return self._set_features_in_place(self._df_infer)

    @classmethod
    def get_model_version(cls):
        return 0


class ModelV1(BaseModelV1):
    def __init__(self, df_infer: pd.DataFrame):
        super().__init__(df_infer=df_infer, model_type=ModelType.RANDOM_FOREST)

    @abstractmethod
    def get_alternative_recommendations(self) -> pd.DataFrame:
        return self._set_features_in_place(self._df_infer)

    @classmethod
    def get_model_version(cls):
        return 1


class ModelV2(BaseModel):
    def __init__(self, df_infer: pd.DataFrame):
        super().__init__(df_infer=df_infer, model_type=ModelType.LIGHTGBM)

    @abstractmethod
    def _set_features_in_place(self, df: pd.DataFrame) -> pd.DataFrame:
        df["model_version"] = 2
        return df

    @abstractmethod
    def get_alternative_recommendations(self) -> pd.DataFrame:
        return self._set_features_in_place(self._df_infer)

    @classmethod
    def get_model_version(cls):
        return 2


if __name__ == "__main__":
    _DF = pd.DataFrame(
        {
            "user_id": ["user_1", "user_1"],
            "offer_id": ["offer_1", "offer_2"],
            "platform": [1, 1],
            "offer_type": [1, 1],
        }
    )
    v0 = ModelV0(_DF.copy())
    print(f"----- {v0.get_model_version()}")
    print(v0.get_alternative_recommendations())

    v1 = ModelV1(_DF.copy())
    print(f"----- {v1.get_model_version()}")
    print(v1.get_alternative_recommendations())

    v2 = ModelV2(_DF.copy())
    print(f"----- {v2.get_model_version()}")
    print(v2.get_alternative_recommendations())
