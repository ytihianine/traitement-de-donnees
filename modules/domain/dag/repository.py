from abc import ABC, abstractmethod
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from modules.domain.dag.model import DagStatus, DBParams, FeatureFlagsEnable


class DagRepository(ABC):
    @abstractmethod
    def get_project_name(self, context: Mapping[str, Any]) -> str:
        """Extract project name from context."""
        ...

    @abstractmethod
    def get_dag_status(self, context: Mapping[str, Any]) -> DagStatus:
        """Extract DAG status from context."""
        ...

    @abstractmethod
    def get_execution_date(
        self, context: Mapping[str, Any], use_tz: bool = False, tz_zone: str = "Europe/Paris"
    ) -> datetime:
        """Extract execution date from context."""
        ...

    @abstractmethod
    def get_db_info(self, context: Mapping[str, Any]) -> DBParams:
        """Extract database info from context."""
        ...

    @abstractmethod
    def get_feature_flags(self, context: Mapping[str, Any]) -> FeatureFlagsEnable:
        """Extract feature flags from context."""
        ...
