from abc import ABCMeta, abstractmethod
import re
import typing
from typing import Optional, Tuple, Union


class Alert(metaclass=ABCMeta):
    _theme_re = re.compile('<b>Тема:</b> Re: omd-dwh \| (.+)\s*\(!(\d+)\).*')

    @staticmethod
    def _search_re(text: str, regexp: typing.re.Pattern[str]) \
            -> Optional[typing.re.Match[str]]:
        for line in text.split('\n'):
            match = regexp.match(line)
            if match:
                return match
        return None

    def _get_theme(self, text: str) -> Union[Tuple[str, str], Tuple[None, None]]:
        for line in text.split('\n'):
            match = self._theme_re.match(line)
            if match:
                return match.group(1), self._get_rm_link(match.group(2))
        return None, None

    @staticmethod
    def _get_rm_link(mr_id: str) -> str:
        return f'https://gitlab.omd.ru/dwh/omd-dwh/merge_requests/{mr_id}'

    @abstractmethod
    def get_result(self, text: str) -> Optional[str]:
        raise NotImplementedError


class AssigneeAlert(Alert):
    _assignee_re = re.compile(
        '.*<p>Assignee changed '
        'from <strong>([^<]+)</strong> '
        'to <strong>([^<]+)</strong>.*'
    )
    _target = 'Petrov Vladimir'

    def get_result(self, text: str) -> Optional[str]:
        match = self._search_re(text, self._assignee_re)
        if match:
            if self._target not in match.groups():
                return None
            mr_name, mr_link = self._get_theme(text)
            return (
                f'{mr_name} {mr_link} Assignee changed from '
                f'*{match.group(1)}* to *{match.group(2)}*'
            )


class MergeDoneAlert(Alert):
    _merge_done_re = re.compile('.+Merge Request !(\d+) was merged.+')

    def get_result(self, text: str) -> Optional[str]:
        match = self._search_re(text, self._merge_done_re)
        if match:
            mr_name, mr_link = self._get_theme(text)
            return f'Merged {mr_name} {mr_link}'
