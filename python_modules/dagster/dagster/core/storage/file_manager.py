from abc import ABC, abstractmethod, abstractproperty

from dagster import check
from dagster.core.types.decorator import usable_as_dagster_type


# pylint: disable=no-init
@usable_as_dagster_type
class FileHandle(ABC):
    """A file handle is a reference to a file.

    Files can be be resident in the local file system, an object store, or any arbitrary place
    where a file can be stored.

    This exists to handle the very common case where you wish to write a computation that reads,
    transforms, and writes files, but where the same code can work in local development as well
    as in a cluster where the files would be stored in globally available object store such as s3.
    """

    @abstractproperty
    def path_desc(self):
        """ This is a properly to return a *representation* of the path for
        diplay purposes. Should not be used in a programatically meaningful
        way beyond display"""
        raise NotImplementedError()


class FileManager(ABC):  # pylint: disable=no-init
    """
    The base class for all file managers in dagster. The file manager is a user-facing
    abstraction that allows a Dagster user to pass files in between solids, and the file
    manager is responsible for marshalling those files to and from the nodes where
    the actual Dagster computation occur.

    If the user does their file manipulations using this abstraction, it is straightforward to write
    a pipeline that executes both:

        (a) in a local development environment with no external dependencies, where files are
            available directly on the filesystem and
        (b) in a cluster environment where those files would need to be on a distributed filesystem
            (e.g. hdfs) or an object store (s3).

    The business logic remains constant and a new implementation of the file manager is swapped out
    based on the system storage specified by the operator.
    """

    @abstractmethod
    def copy_handle_to_local_temp(self, file_handle):
        """
        Take a file handle and make it available as a local temp file. Returns a path.

        In an implementation like an ``S3FileManager``, this would download the file from s3
        to local filesystem, to files created (typically) by the python tempfile module.

        These temp files are *not* guaranteed to be able across solid boundaries. For
        files that must work across solid boundaries, use the read, read_data, write, and
        write_data methods on this class.

        Args:
            file_handle (FileHandle): The file handle to make available as a local temp file.

        Returns:
            str: Path to the temp file.
        """
        raise NotImplementedError()

    @abstractmethod
    def delete_local_temp(self):
        """
        Delete all the local temporary files created by ``copy_handle_to_local_temp``. This should
        typically only be called by framework implementors.
        """
        raise NotImplementedError()

    @abstractmethod
    def read(self, file_handle, mode="rb"):
        """Return a file-like stream for the file handle. Defaults to binary mode read.
        This may incur an expensive network call for file managers backed by object stores
        such as s3.

        Args:
            file_handle (FileHandle): The file handle to make available as a stream.
            mode (str): The mode in which to open the file. Default: ``'rb'``.

        Returns:
            Union[IOBytes, IOString]: A file-like stream.
        """
        raise NotImplementedError()

    @abstractmethod
    def read_data(self, file_handle):
        """Return the bytes for a given file handle. This may incur an expensive network
        call for file managers backed by object stores such as s3.

        Args:
            file_handle (FileHandle): The file handle for which to return bytes.

        Returns:
            bytes: Bytes for a given file handle.
        """
        raise NotImplementedError()

    @abstractmethod
    def write(self, file_obj, mode="wb", ext=None, file_key: str = None):
        """Write the bytes contained within the given ``file_obj`` into the file manager.
        This returns a :py:class:`~dagster.FileHandle` corresponding to the newly created file.

        File managers typically return a subclass of :py:class:`~dagster.FileHandle` appropriate for
        their implementation: e.g., a
        :py:class:`~dagster.core.storage.file_manager.LocalFileManager` returns a
        :py:class:`~dagster.LocalFileHandle`, an :py:class:`~dagster_aws.S3FileManager` returns an
        :py:class:`~dagster_aws.S3FileHandle`, and so forth.

        Args:
            file_obj (Union[IOBytes, IOString]): A file-like object.
            mode (Optional[str]): The mode in which to write the file into storage. Default: ``'wb'``.
            ext (Optional[str]): For file managers that support file extensions, the extension with
                which to write the file. Default: ``None``.
            file_key (str): A unique identifier for the file. If none is provided, will generate
                a random one.

        Returns:
            FileHandle: A handle to the newly created file.
        """
        raise NotImplementedError()

    @abstractmethod
    def write_data(self, data, ext=None, file_key: str = None):
        """Write raw bytes into storage.

        Args:
            data (bytes): The bytes to write into storage.
            ext (Optional[str]): For file managers that support file extensions, the extension with
                which to write the file. Default: ``None``.
            file_key (str): A unique identifier for the file. If none is provided, will generate
                a random one.

        Returns:
            FileHandle: A handle to the newly created file.
        """
        raise NotImplementedError()

    def get_file_handle(self, file_key, ext):
        """Gets a handle to the file that would be created with a call to write_data or write, with
        the given file_key and extension.

        Invocations to this method with the same file_key and extention should always return the
        same file handle.

        Args:
            file_key (str): A unique identifier for the file.
            ext (Optional[str]): For file managers that support file extensions, the extension with
                which to write the file. Default: ``None``.

        Returns:
            FileHandle: A handle to the file that would be created with a call to write_data or
                write, with the given file_key and extension.
        """
        raise NotImplementedError()


def check_file_like_obj(obj):
    check.invariant(obj and hasattr(obj, "read") and hasattr(obj, "write"))
