from abc import ABC, abstractmethod


class Estimator(ABC):
    """An estimator for the speed and ETA of a running task"""

    @abstractmethod
    def tick(self, advance: int = 1) -> None:
        """Advance the estimator by a custom amount of steps

        Args:
            advance (int, optional): The number of steps to advance. Defaults to 1.
        """
        pass

    @abstractmethod
    def reset(self, total: int) -> None:
        """Reset the estimator to a new total number of steps

        Args:
            total (int): The total number of steps
        """
        pass

    @property
    @abstractmethod
    def eta(self) -> float:
        """The estimated time remaining in seconds"""
        pass

    @property
    @abstractmethod
    def speed(self) -> float:
        """The estimated speed in steps per second"""
        pass

    @property
    @abstractmethod
    def elapsed(self) -> float:
        """The elapsed time in seconds"""
        pass

    @property
    @abstractmethod
    def start_time(self) -> float:
        """The start time in seconds"""
        pass
