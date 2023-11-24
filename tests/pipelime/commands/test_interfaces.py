import typing as t
from contextlib import nullcontext
from pathlib import Path

import pytest
from pydantic import ValidationError, create_model

import pipelime.commands.interfaces as plint
import pipelime.sequences.pipes.operations as plops


class TestInterface:
    def _check_description(
        self, model_cls, interf_class, no_desc_field, user_desc_field, user_desc, flags
    ):
        nodesc = model_cls.__fields__[no_desc_field].field_info.description
        udesc = model_cls.__fields__[user_desc_field].field_info.description
        extra = model_cls.__fields__[user_desc_field].field_info.extra

        assert interf_class._default_type_description is not None
        assert interf_class._compact_form is not None
        assert nodesc is not None
        assert udesc is not None
        assert interf_class._default_type_description in nodesc
        assert interf_class._default_type_description not in udesc
        assert interf_class._compact_form in nodesc
        assert interf_class._compact_form in udesc
        assert user_desc in udesc
        for k, v in flags.items():
            assert k in extra
            assert extra[k] == v

    def _check_any_call(
        self, model_cls, value_check_fn, opt_dict, opt_parse_list, should_fail
    ):
        ctxman = pytest.raises if should_fail else nullcontext

        with ctxman(ValidationError):
            m = model_cls(**opt_dict)
            value_check_fn(m, False)
        for opt in opt_parse_list:
            print(opt)
            with ctxman(ValidationError):
                m = model_cls.parse_obj(opt)
                value_check_fn(m, True)
        with ctxman(ValidationError):
            m = model_cls.parse_obj(opt_dict)
            value_check_fn(m, False)
        try:
            m = model_cls.parse_obj({k: getattr(m, k) for k in model_cls.__fields__})
            value_check_fn(m, False)
            assert not should_fail
        except NameError:
            assert should_fail

    def _standard_checks(
        self, *, interf_cls, interf_compact_list, should_fail, out_of_compact, **kwargs
    ):
        _MyModel = create_model(
            "_MyModel",
            f1=(interf_cls, interf_cls.pyd_field()),
            f2=(
                t.Optional[interf_cls],
                interf_cls.pyd_field(
                    is_required=False,
                    description="f2",
                    extra_flag_1="extra_1",
                    extra_flag_2=42,
                ),
            ),
        )

        self._check_description(
            _MyModel,
            interf_cls,
            "f1",
            "f2",
            "f2",
            {"extra_flag_1": "extra_1", "extra_flag_2": 42},
        )

        opt_dict = {"f1": {}}
        for k, v in kwargs.items():
            if v is not None:
                opt_dict["f1"][k] = v

        # get default values
        default_values = {}
        for k, v in interf_cls.__fields__.items():
            default_values[k] = v.get_default()
        for k, v in kwargs.items():
            if v is None:
                kwargs[k] = default_values[k]

        def _check_deep(x, y):
            from pipelime.utils.pydantic_types import YamlInput

            if isinstance(x, t.Sequence) and not isinstance(x, str):
                assert isinstance(y, t.Sequence)
                assert len(x) == len(y)
                for xx, yy in zip(x, y):
                    _check_deep(xx, yy)
            elif isinstance(x, t.Mapping):
                assert isinstance(y, t.Mapping)
                assert set(x.keys()) == set(y.keys())
                for k, xx in x.items():
                    _check_deep(xx, y[k])
            elif isinstance(x, YamlInput):
                _check_deep(x.value, y)
            else:
                assert x == (Path(y).resolve().absolute() if isinstance(x, Path) else y)

        def _check_values(my_model, from_opt_str):
            for k, v in kwargs.items():
                if not from_opt_str or k not in out_of_compact:
                    a = getattr(my_model.f1, k)
                    _check_deep(a, v)
            if my_model.f2 is not None:
                for k, v in default_values.items():
                    a = getattr(my_model.f2, k)
                    _check_deep(a, v)

        self._check_any_call(
            _MyModel,
            _check_values,
            opt_dict,
            [{"f1": o} for o in interf_compact_list if o is not None],
            should_fail,
        )


class TestGrabberInterfaces(TestInterface):
    @pytest.mark.parametrize("nproc", [-10, -1, 0, 1, 10, None])
    @pytest.mark.parametrize("pref", [1, 2, 10, None])
    def test_valid(self, nproc: t.Optional[int], pref: t.Optional[int]):
        opt_str = ""
        opt_int = None
        if nproc is not None:
            opt_str = f"{nproc}"
        if pref is None:
            opt_int = nproc
        else:
            opt_str += f",{pref}"
        self._standard_checks(
            interf_cls=plint.GrabberInterface,
            interf_compact_list=[opt_str, opt_int],
            out_of_compact=[],
            should_fail=False,
            num_workers=nproc,
            prefetch=pref,
        )

    @pytest.mark.parametrize("prefetch", [-7, -1, 0])
    def test_invalid(self, prefetch: int):
        self._standard_checks(
            interf_cls=plint.GrabberInterface,
            interf_compact_list=[f"1,{prefetch}"],
            out_of_compact=[],
            should_fail=True,
            num_workers=1,
            prefetch=prefetch,
        )
        with pytest.raises(ValueError):
            plint.GrabberInterface.validate([1, 2, 3])


class TestInputDataset(TestInterface):
    @pytest.mark.parametrize(
        "folder,check_cls",
        [
            (None, True),
            (
                Path(__file__).parent
                / "../../sample_data/datasets/underfolder_minimnist",
                True,
            ),
            (Path(), False),
            ("", False),
            (
                (
                    Path(__file__).parent
                    / "../../sample_data/datasets/underfolder_minimnist"
                ).as_posix(),
                True,
            ),
        ],
    )
    @pytest.mark.parametrize("merge_root_items", [True, False, None])
    @pytest.mark.parametrize("skip_empty", [True, False, None])
    @pytest.mark.parametrize(
        "pipe,pipe_cls",
        [
            (None, None),
            ({"select": {"indexes": [1, 2]}}, plops.IndexSelectionSequence),
            (["enumerate"], plops.EnumeratedSequence),
        ],
    )
    def test_valid(
        self,
        folder: t.Union[Path, str, None],
        merge_root_items: t.Optional[bool],
        skip_empty: t.Optional[bool],
        pipe: t.Optional[t.Union[t.Mapping, t.Sequence]],
        pipe_cls: t.Optional[t.Type[plops.PipedSequenceBase]],
        check_cls: bool,
    ):
        from pipelime.sequences.sources.readers import UnderfolderReader
        from pipelime.sequences.sources.toy_dataset import ToyDataset

        if folder is None:
            opt_str = []
            if pipe is not None:
                pipe_def = [{"toy_dataset": [10]}]
                if isinstance(pipe, t.Mapping):
                    pipe_def.append(pipe)  # type: ignore
                else:
                    pipe_def.extend(pipe)
            else:
                pipe_def = None
        else:
            # merge_root_items is always True when using the compact form
            if merge_root_items:
                opt_str = f"{str(folder)}"
                if skip_empty is not None:
                    opt_str += f",{str(skip_empty)}"
            else:
                opt_str = None
            opt_str = [opt_str]
            pipe_def = pipe

        self._standard_checks(
            interf_cls=plint.InputDatasetInterface,
            interf_compact_list=opt_str,
            should_fail=folder is None and pipe_def is None,
            out_of_compact=["pipe"],
            folder=folder,
            merge_root_items=merge_root_items,
            skip_empty=skip_empty,
            pipe=pipe_def,
        )

        if check_cls and (folder is not None or pipe_def is not None):
            reader = plint.InputDatasetInterface(
                folder=folder,
                merge_root_items=True if merge_root_items is None else merge_root_items,
                skip_empty=False if skip_empty is None else skip_empty,
                pipe=pipe_def,
                schema=None,
            ).create_reader()
            if folder is not None:
                if pipe_cls is not None:
                    assert isinstance(reader, pipe_cls)
                    reader = reader.source
                if skip_empty:
                    assert isinstance(reader, plops.FilteredSequence)
                    reader = reader.source
                assert isinstance(reader, UnderfolderReader)
            else:
                if skip_empty:
                    assert isinstance(reader, plops.FilteredSequence)
                    reader = reader.source
                if pipe_cls is not None:
                    assert isinstance(reader, pipe_cls)
                    assert isinstance(reader.source, ToyDataset)

    def test_invalid(self):
        self._standard_checks(
            interf_cls=plint.InputDatasetInterface,
            interf_compact_list=["/path/to/folder,42"],
            out_of_compact=[],
            should_fail=True,
            folder=None,
            merge_root_items=None,
            skip_empty=None,
        )
        with pytest.raises(ValueError):
            plint.InputDatasetInterface.validate([1, 2, 3])


class TestOutputDataset(TestInterface):
    @pytest.mark.parametrize(
        "folder,check_cls",
        [
            (None, True),
            (
                "fake_output",
                True,
            ),
            (Path(), False),
            ("", False),
            (
                Path("fake_output"),
                True,
            ),
        ],
    )
    @pytest.mark.parametrize("zfill", [0, 10, None])
    @pytest.mark.parametrize("exists_ok", [True, False, None])
    @pytest.mark.parametrize(
        "pipe,pipe_cls",
        [
            (None, None),
            ({"select": {"indexes": [1, 2]}}, plops.IndexSelectionSequence),
            (["enumerate"], plops.EnumeratedSequence),
        ],
    )
    def test_valid(
        self,
        folder: t.Union[Path, str, None],
        zfill: t.Optional[int],
        exists_ok: bool,
        pipe: t.Optional[t.Union[t.Mapping, t.Sequence]],
        pipe_cls: t.Optional[t.Type[plops.PipedSequenceBase]],
        check_cls: bool,
        tmp_path: Path,
    ):
        from pipelime.sequences.pipes.writers import UnderfolderWriter
        from pipelime.sequences.sources.toy_dataset import ToyDataset

        if folder is None:
            opt_str = []
            should_fail = pipe is None
        else:
            if check_cls:
                p = tmp_path / folder
                folder = p.as_posix() if isinstance(folder, str) else p

            # zfill is always None when using the compact form
            if zfill is None:
                opt_str = f"{str(folder)}"
                if exists_ok is not None:
                    opt_str += f",{str(exists_ok)}"
            else:
                opt_str = None
            opt_str = [opt_str]
            should_fail = exists_ok is not True and Path(folder).exists()

        self._standard_checks(
            interf_cls=plint.OutputDatasetInterface,
            interf_compact_list=opt_str,
            should_fail=should_fail,
            out_of_compact=["pipe"],
            folder=folder,
            zfill=zfill,
            exists_ok=exists_ok,
            pipe=pipe,
        )

        if check_cls and (folder is not None or pipe is not None):
            writer = plint.OutputDatasetInterface(
                folder=folder,
                zfill=zfill,
                exists_ok=True if exists_ok is None else exists_ok,
                pipe=pipe,
                schema=None,
            ).append_writer(ToyDataset(10))
            if folder is not None:
                assert isinstance(writer, UnderfolderWriter)
                writer = writer.source
            if pipe_cls is not None:
                assert isinstance(writer, pipe_cls)
                writer = writer.source

    def test_invalid(self):
        self._standard_checks(
            interf_cls=plint.OutputDatasetInterface,
            interf_compact_list=["/path/to/folder,42"],
            out_of_compact=[],
            should_fail=True,
            folder=None,
            zfill=None,
            exists_ok=None,
        )
        with pytest.raises(ValueError):
            plint.OutputDatasetInterface.validate([1, 2, 3])


class TestRemoteInterface(TestInterface):
    @pytest.mark.parametrize(
        "url",
        [
            "s3://user:password@host:42/bucket?kw1=arg1:kw2=arg2",
            {
                "scheme": "s3",
                "user": "user",
                "password": "password",
                "host": "host",
                "port": 42,
                "bucket": "bucket",
                "args": {"kw1": "arg1", "kw2": "arg2"},
            },
        ],
    )
    def test_valid(self, url: t.Union[str, t.Mapping]):
        self._standard_checks(
            interf_cls=plint.RemoteInterface,
            interf_compact_list=[url if isinstance(url, str) else None],
            out_of_compact=[],
            should_fail=False,
            url=url,
        )

    def test_invalid(self):
        self._standard_checks(
            interf_cls=plint.RemoteInterface,
            interf_compact_list=[42],
            should_fail=True,
            out_of_compact=[],
            url={"not": "valid"},
        )
        with pytest.raises(ValueError):
            plint.RemoteInterface.validate([1, 2, 3])
