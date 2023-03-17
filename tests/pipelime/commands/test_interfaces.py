import pytest
import typing as t
from pathlib import Path
from contextlib import nullcontext
from pydantic import ValidationError, create_model
import pipelime.commands.interfaces as plint


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
            value_check_fn(m)
        for opt in opt_parse_list:
            print(opt)
            with ctxman(ValidationError):
                m = model_cls.parse_obj(opt)
                value_check_fn(m)
        with ctxman(ValidationError):
            m = model_cls.parse_obj(opt_dict)
            value_check_fn(m)
        try:
            m = model_cls.parse_obj({k: getattr(m, k) for k in model_cls.__fields__})
            value_check_fn(m)
            assert not should_fail
        except NameError:
            assert should_fail

    def _standard_checks(
        self, *, interf_cls, interf_compact_list, should_fail, **kwargs
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

        def _check_values(my_model):
            for k, v in kwargs.items():
                a = getattr(my_model.f1, k)
                assert a == (Path(v) if isinstance(a, Path) else v)
            if my_model.f2 is not None:
                for k, v in default_values.items():
                    a = getattr(my_model.f2, k)
                    assert a == (Path(v) if isinstance(a, Path) else v)

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
            should_fail=False,
            num_workers=nproc,
            prefetch=pref,
        )

    @pytest.mark.parametrize("prefetch", [-7, -1, 0])
    def test_invalid(self, prefetch: int):
        self._standard_checks(
            interf_cls=plint.GrabberInterface,
            interf_compact_list=[f"1,{prefetch}"],
            should_fail=True,
            num_workers=1,
            prefetch=prefetch,
        )
        with pytest.raises(ValueError):
            plint.GrabberInterface.validate([1, 2, 3])


class TestInputDataset(TestInterface):
    @pytest.mark.parametrize(
        "folder", [Path("path/to/folder"), Path(), "", "path/to/folder"]
    )
    @pytest.mark.parametrize("merge_root_items", [True, False, None])
    @pytest.mark.parametrize("skip_empty", [True, False, None])
    def test_valid(
        self,
        folder: t.Union[Path, str],
        merge_root_items: t.Optional[bool],
        skip_empty: t.Optional[bool],
    ):
        absf = Path(folder).absolute()
        folder = str(absf) if isinstance(folder, str) else absf

        # merge_root_items is always True when using the compact form
        if merge_root_items:
            opt_str = f"{str(absf)}"
            if skip_empty is not None:
                opt_str += f",{str(skip_empty)}"
        else:
            opt_str = None

        self._standard_checks(
            interf_cls=plint.InputDatasetInterface,
            interf_compact_list=[opt_str],
            should_fail=False,
            folder=folder,
            merge_root_items=merge_root_items,
            skip_empty=skip_empty,
        )

    def test_invalid(self):
        self._standard_checks(
            interf_cls=plint.InputDatasetInterface,
            interf_compact_list=["/path/to/folder,42"],
            should_fail=True,
            folder=None,
            merge_root_items=None,
            skip_empty=None,
        )
        with pytest.raises(ValueError):
            plint.InputDatasetInterface.validate([1, 2, 3])


class TestOutputDataset(TestInterface):
    @pytest.mark.parametrize(
        "folder", [Path("path/to/folder"), Path(), "", "path/to/folder"]
    )
    @pytest.mark.parametrize("zfill", [0, 10, None])
    @pytest.mark.parametrize("exists_ok", [True, False, None])
    def test_valid(
        self, folder: t.Union[Path, str], zfill: t.Optional[int], exists_ok: bool
    ):
        absf = Path(folder).absolute()
        folder = str(absf) if isinstance(folder, str) else absf

        # zfill is always None when using the compact form
        if zfill is None:
            opt_str = f"{str(absf)}"
            if exists_ok is not None:
                opt_str += f",{str(exists_ok)}"
        else:
            opt_str = None

        self._standard_checks(
            interf_cls=plint.OutputDatasetInterface,
            interf_compact_list=[opt_str],
            should_fail=exists_ok is not True and Path(folder).exists(),
            folder=folder,
            zfill=zfill,
            exists_ok=exists_ok,
        )

    def test_invalid(self):
        self._standard_checks(
            interf_cls=plint.OutputDatasetInterface,
            interf_compact_list=["/path/to/folder,42"],
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
            should_fail=False,
            url=url,
        )

    def test_invalid(self):
        self._standard_checks(
            interf_cls=plint.RemoteInterface,
            interf_compact_list=[42],
            should_fail=True,
            url={"not": "valid"},
        )
        with pytest.raises(ValueError):
            plint.RemoteInterface.validate([1, 2, 3])
