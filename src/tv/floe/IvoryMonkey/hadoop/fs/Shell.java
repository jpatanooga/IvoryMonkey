package tv.floe.IvoryMonkey.hadoop.fs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.shell.Count;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Simple shell class in the same spirit of "FsShell.java" of hadoop to test
 * checksumming functionality.
 * 
 * Should be run as hadoop jar:
 * 
 * To calculate a local checksum:
 * 
 * hadoop jar IvoryMonkey_v0_1.jar tv.floe.IvoryMonkey.hadoop.fs.Shell -hdfsChksm_Local file:///dir/dir/filename
 * 
 * Which will return:
 * 
 * checksum: MD5-of-131072MD5-of-512CRC32:271ff9d23897e20c82eb2414aa984671
 * 
 * 
 * To calculate a remote checksum:
 * 
 * hadoop jar IvoryMonkey_v0_1.jar tv.floe.IvoryMonkey.hadoop.fs.Shell -hdfsChksm_Remote /dir/dir/filename
 * 
 * Which will return:
 * 
 * ExternalHDFSChecksumGenerator > Get Checksum For:
 * /data/IvoryMonkey/jdk-rpm.bin from hdfs://localhost:8020
 * ExternalHDFSChecksumGenerator > DFS Initialized! remote hdfs checksum:
 * MD5-of-131072MD5-of-512CRC32:271ff9d23897e20c82eb2414aa984671
 * 
 * 
 * 
 * @author jpatterson
 * 
 */
public class Shell extends Configured implements Tool {

	protected FileSystem fs;
	private Trash trash;

	/**
	   */
	public Shell() {
		this(null);
	}

	public Shell(Configuration conf) {
		super(conf);
		fs = null;
		trash = null;
	}

	protected void init() throws IOException {

		if (getConf() == null) {
			System.out.println("init > getConf() returns null!");
			return;
		}

		getConf().setQuietMode(true);
		if (this.fs == null) {
			this.fs = FileSystem.get(getConf());
		}
		if (this.trash == null) {
			this.trash = new Trash(getConf());
		}
	}

	/**
	 * Displays format of commands.
	 * 
	 */
	private static void printUsage(String cmd) {
		String prefix = "Usage: java " + Shell.class.getSimpleName();
		if ("-hdfsChksm_Remote".equals(cmd)) {

			System.err.println("Usage: java Shell -hdfsChksm_Remote <path>");

		} else if ("-hdfsChksm_Local".equals(cmd)) {

				System.err.println("Usage: java Shell -hdfsChksm_Local <path>");
			
			
		} else if ("-hdfsChksmCmpToLocal".equals(cmd)) {

			System.err
					.println("Usage: java Shell -hdfsChksmCmpToLocal <local-path> <hdfs-path>");

		} else {
			System.err.println("Usage: java Shell");

			// new commands
			System.err.println("           [-hdfsChksm_Local <path>]");
			System.err.println("           [-hdfsChksm_Remote <path>]");
			System.err
					.println("           [-hdfsChksmCmpToLocal <local-path> <hdfs-path>]");

			System.err.println("           [-help [cmd]]");
			System.err.println();
			ToolRunner.printGenericCommandUsage(System.err);
		}
	}

	public int run(String argv[]) throws Exception {

		if (argv.length < 1) {
			printUsage("");
			return -1;
		}

		int exitCode = -1;
		int i = 0;
		String cmd = argv[i++];

		//
		// verify that we have enough command line parameters
		//
		if ("-hdfsChksm_Remote".equals(cmd) || "-hdfsChksm_Local".equals(cmd)) {
			if (argv.length < 2) {
				printUsage(cmd);
				return exitCode;
			}
		}

		// initialize FsShell
		try {
			init();
		} catch (RPC.VersionMismatch v) {
			System.err.println("Version Mismatch between client and server"
					+ "... command aborted.");
			return exitCode;
		} catch (IOException e) {
			System.err.println("Bad connection to FS. command aborted.");
			return exitCode;
		}

		exitCode = 0;
		try {
			if ("-hdfsChksm_Remote".equals(cmd)) {

				String path = argv[i++];

				ExternalHDFSChecksumGenerator gen = new ExternalHDFSChecksumGenerator(
						this.getConf());

				MD5MD5CRC32FileChecksum cf = gen.getHDFSChecksum(path);
				System.out.println("remote hdfs checksum: " + cf);

			} else if ("-hdfsChksm_Local".equals(cmd)) {

				String hdfs_path = argv[i++];

				ExternalHDFSChecksumGenerator gen = new ExternalHDFSChecksumGenerator(
						this.getConf());

				// TestHadoopFilesystem( hdfs_path );

				MD5MD5CRC32FileChecksum cf = gen
						.getLocalFilesystemHDFSStyleChecksum(hdfs_path, 512,
								64 * 1024 * 1024);
				System.out.println("checksum: " + cf);

				/*
				 * } else if ("-hdfsChksm".equals(cmd)) {
				 * 
				 * String hdfs_path = argv[i++];
				 * 
				 * ExternalHDFSChecksumGenerator gen = new
				 * ExternalHDFSChecksumGenerator();
				 * 
				 * gen.GetHDFSChecksumDebug( hdfs_path );
				 */
			} else if ("-hdfsChksm_Alt".equals(cmd)) {

			} else if ("-hdfsChksmCmpToLocal".equals(cmd)) {

				String local_path = argv[i++];
				String hdfs_path = argv[i++];

				ExternalHDFSChecksumGenerator gen = new ExternalHDFSChecksumGenerator();

				System.out.println("Would Compare Checksum - Local: "
						+ local_path + " against Hdfs: " + hdfs_path);

				MD5MD5CRC32FileChecksum cf = gen
						.getLocalFilesystemHDFSStyleChecksum(hdfs_path, 512,
								64 * 1024 * 1024);
				System.out.println("local checksum: " + cf);

				MD5MD5CRC32FileChecksum cf_hdfs = gen
						.getHDFSChecksum(hdfs_path);
				System.out.println("hdfs checksum: " + cf_hdfs);

				if (cf.equals(cf_hdfs)) {

					System.out.println("FsShell_Work > They match.");

				} else {

					System.out.println("FsShell_Work > They do not match.");

				}

			} else {
				exitCode = -1;
				System.err.println(cmd.substring(1) + ": Unknown command");
				printUsage("");
			}
		} catch (IllegalArgumentException arge) {
			exitCode = -1;
			System.err.println(cmd.substring(1) + ": "
					+ arge.getLocalizedMessage());
			printUsage(cmd);

		} catch (Exception re) {
			exitCode = -1;
			System.err.println("Exception: " + cmd.substring(1) + ": "
					+ re.getLocalizedMessage() + ": " + re);
		} finally {
		}
		return exitCode;
	}

	public void close() throws IOException {
		if (fs != null) {
			fs.close();
			fs = null;
		}
	}

	/**
	 * main() has some simple utility methods
	 */
	public static void main(String argv[]) throws Exception {

		Shell shell = new Shell();

		int res = 0;
		try {
			res = ToolRunner.run(shell, argv);
			// shell.TestHadoopFilesystem();
		} finally {
			shell.close();
		}
		System.exit(res);
	}
}
