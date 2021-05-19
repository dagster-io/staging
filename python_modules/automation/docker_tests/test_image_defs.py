import pytest
from automation.docker.image_defs import get_image


def test_get_image(tmpdir):
    assert get_image("k8s-example")

    with pytest.raises(Exception) as e:
        get_image("new-image", base_path=tmpdir)
    assert "No such file or directory" in str(e.value)

    (tmpdir / "images").mkdir()

    with pytest.raises(Exception) as e:
        get_image("new-image", base_path=tmpdir)
    assert "could not find image new-image" in str(e.value)

    (tmpdir / "images" / "new-image").mkdir()
    (tmpdir / "images" / "new-image" / "Dockerfile").write("FROM hello-world")

    assert get_image("new-image", base_path=tmpdir)
