package spark.support;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;

public class WindowsSafeRawLocalFileSystem extends RawLocalFileSystem {
    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        File localFile = pathToFile(path);

        if (!localFile.exists()) {
            throw new FileNotFoundException("File " + path + " does not exist");
        }

        if (!localFile.isDirectory()) {
            return new FileStatus[] {getFileStatus(path)};
        }

        File[] children = localFile.listFiles();
        if (children == null) {
            if (!localFile.canRead()) {
                throw new AccessDeniedException(localFile.toString(), null, "Permission denied");
            }

            throw new IOException("Invalid directory or I/O error occurred for dir: " + localFile);
        }

        FileStatus[] statuses = new FileStatus[children.length];
        for (int index = 0; index < children.length; index++) {
            statuses[index] = getFileStatus(new Path(path, children[index].getName()));
        }

        return statuses;
    }
}
