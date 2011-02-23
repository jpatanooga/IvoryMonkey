package tv.floe.IvoryMonkey.hadoop.fs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.util.DataChecksum;

/**
 * 
 * This is a single threaded implementation of HDFS's checksumming mechanics
 * where it creates a MD5 hash of all of the MD5 block hashes of the CRC32's
 * that hdfs keeps for every 512 bytes it stores.
 * 
 * In general HDFS keeps an extra 4 bytes as a CRC for each 512 bytes of block
 * data it stores.
 * 
 * ToDo:
 * 
 * - Document hadoop's execution path for calculating checksum code in hdfs
 * across machines in parallel
 * 
 * Errata:
 * 
 * - we discovered a bug in the implementation during the development of this
 * class for the openPDC - we filed the bug with AC Murthy and Tsz Wo of Yahoo:
 * http://issues.apache.org/jira/browse/HDFS-772 - the bug is described as"The fileMD5 is computed with the entire byte array returning by md5out.getData(). However, data are valid only up to md5out.getLength(). Therefore, the currently implementation of the algorithm compute fileMD5 with extra padding."
 * - currently this code works with CDH3b3 since the padding issue is consistent
 * 
 * Usage
 * 
 * - should be run from the Shell class - Shell class should be run as a Hadoop
 * jar to get proper config for talking to your cluster - this class can be very
 * handy in cases where a large file is moved multiple hops into hdfs and may
 * not directly use "hadoop fs -put" - in the case of the openPDC, we used a
 * customized FTP interface to talk to hdfs so we had a "2-hop" issue - we
 * pulled the checksum from hdfs and then used this code to calculate the
 * checksum on the client side to make sure we had the same file on both sides.
 * 
 * 
 * 
 * @author jpatterson
 * 
 */
public class ExternalHDFSChecksumGenerator extends Configured {

	protected FileSystem fs;

	public ExternalHDFSChecksumGenerator() {
		this(null);
	}

	public ExternalHDFSChecksumGenerator(Configuration conf) {
		super(conf);
		fs = null;
		// trash = null;
	}

	protected void init() throws IOException {

		if (getConf() == null) {
			System.out
					.println("ExternalHDFSChecksumGenerator > init > getConf() returns null!");
			return;
		}

		getConf().setQuietMode(true);
		if (this.fs == null) {
			this.fs = FileSystem.get(getConf());
		}

	}
/*
	public void debug_GetHDFSChecksum(String str_HDFS_Path) throws IOException {

		String hdfs_uri = getConf().get("fs.default.name");

		String hdfs_path = str_HDFS_Path; // ex:
											// "hdfs://socdvmhdfs1:9000/hadoop/test"
											// or
											// "hdfs://socdvmhdfs1:9000/hadoop/test2"

		System.out.println("ExternalHDFSChecksumGenerator > Get Checksum For: "
				+ hdfs_path);

		this.fs = new DistributedFileSystem();
		Configuration conf = new Configuration();
		conf.set("hadoop.job.ugi", "hadoop,supergroup");
		try {
			this.fs.initialize(new URI(hdfs_uri), conf);

			System.out
					.println("ExternalHDFSChecksumGenerator > DFS Initialized! ");

		} catch (Exception e) {
			System.out
					.println("ExternalHDFSChecksumGenerator > DFS Initialization error! "
							+ e.toString());
		}

		MD5MD5CRC32FileChecksum md5 = (MD5MD5CRC32FileChecksum) this.fs
				.getFileChecksum(new Path(hdfs_path));

		if (null != md5) {

			String hashWithAlgo = md5.toString();
			String strReturnValue = hashWithAlgo.substring(hashWithAlgo
					.indexOf(":"));

			// System.out.println( "FsShell_Work > algo:md5: " + md5.toString()
			// );
			System.out.println("ExternalHDFSChecksumGenerator > md5: "
					+ strReturnValue);

		} else {

			System.out
					.println("ExternalHDFSChecksumGenerator > ERROR > md5 was null!");

		}

	}
*/
	/**
	 * This function pulls the checksum from hdfs remotely.
	 * 
	 * @param hdfs_path
	 * @return
	 * @throws IOException
	 */
	public MD5MD5CRC32FileChecksum getHDFSChecksum(String hdfs_path)
			throws IOException {

		MD5MD5CRC32FileChecksum returnChecksum = null;
		String hdfs_uri = getConf().get("fs.default.name");

		System.out.println("ExternalHDFSChecksumGenerator > Get Checksum For: "
				+ hdfs_path + " from " + hdfs_uri);

		this.fs = new DistributedFileSystem();
		Configuration conf = new Configuration();
		conf.set("hadoop.job.ugi", "hadoop,supergroup");
		try {
			this.fs.initialize(new URI(hdfs_uri), conf);

			System.out
					.println("ExternalHDFSChecksumGenerator > DFS Initialized! ");

			// this.fs.get

		} catch (Exception e) {
			// log.error("DFS Initialization error", e);
			System.out
					.println("ExternalHDFSChecksumGenerator > DFS Initialization error! "
							+ e.toString());
		}

		returnChecksum = (MD5MD5CRC32FileChecksum) this.fs
				.getFileChecksum(new Path(hdfs_path));

		/*
		 * if ( null != md5 ) {
		 * 
		 * String hashWithAlgo = md5.toString(); String strReturnValue =
		 * hashWithAlgo.substring( hashWithAlgo.indexOf(":") );
		 * 
		 * System.out.println( "FsShell_Work > algo:md5: " + md5.toString() );
		 * System.out.println( "FsShell_Work > md5: " + strReturnValue );
		 * //System.out.println( "openPDC > md5.getBytes().length: " +
		 * md5.getBytes().length );
		 * 
		 * //return strReturnValue; //System.out.println( "MD5 > " +
		 * strReturnValue );
		 * 
		 * } else {
		 * 
		 * System.out.println( "ERROR > md5 was null!" );
		 * 
		 * }
		 */
		return returnChecksum;

	}

	/**
	 * 
	 * This is the function that calculates the hdfs-style checksum for a local file in the same way that
	 * hdfs does it in a parallel fashion on all of the blocks in hdsf.
	 * 
	 * @param strPath
	 * @param bytesPerCRC
	 * @param lBlockSize
	 * @return
	 * @throws IOException
	 */
	public MD5MD5CRC32FileChecksum getLocalFilesystemHDFSStyleChecksum(
			String strPath, int bytesPerCRC, long lBlockSize)
			throws IOException {

		Path srcPath = new Path(strPath);
		FileSystem srcFs = srcPath.getFileSystem(getConf());
		long lFileSize = 0;
		int iBlockCount = 0;
		DataOutputBuffer md5outDataBuffer = new DataOutputBuffer();
		DataChecksum chksm = DataChecksum.newDataChecksum(
				DataChecksum.CHECKSUM_CRC32, 512);
		InputStream in = null;
		MD5MD5CRC32FileChecksum returnChecksum = null;
		long crc_per_block = lBlockSize / bytesPerCRC;

		if (null == srcFs) {
			System.out.println("srcFs is null! ");
		} else {

			// FileStatus f_stats = srcFs.getFileStatus( srcPath );
			lFileSize = srcFs.getFileStatus(srcPath).getLen();
			iBlockCount = (int) Math.ceil((double) lFileSize
					/ (double) lBlockSize);

			// System.out.println( "Debug > getLen == " + f_stats.getLen() +
			// " bytes" );
			// System.out.println( "Debug > iBlockCount == " + iBlockCount );

		}

		if (srcFs.getFileStatus(srcPath).isDir()) {

			throw new IOException("Cannot compute local hdfs hash, " + srcPath
					+ " is a directory! ");

		}

		try {

			in = srcFs.open(srcPath);
			long lTotalBytesRead = 0;

			for (int x = 0; x < iBlockCount; x++) {

				ByteArrayOutputStream ar_CRC_Bytes = new ByteArrayOutputStream();

				byte crc[] = new byte[4];
				byte buf[] = new byte[512];

				try {

					int bytesRead = 0;

					while ((bytesRead = in.read(buf)) > 0) {

						lTotalBytesRead += bytesRead;

						chksm.reset();
						chksm.update(buf, 0, bytesRead);
						chksm.writeValue(crc, 0, true);
						ar_CRC_Bytes.write(crc);

						if (lTotalBytesRead >= (x + 1) * lBlockSize) {

							break;
						}

					} // while

					DataInputStream inputStream = new DataInputStream(
							new ByteArrayInputStream(ar_CRC_Bytes.toByteArray()));

					// this actually computes one ---- run on the server
					// (DataXceiver) side
					final MD5Hash md5_dataxceiver = MD5Hash.digest(inputStream);
					md5_dataxceiver.write(md5outDataBuffer);

				} catch (IOException e) {

					e.printStackTrace();

				} catch (Exception e) {

					e.printStackTrace();

				}

			} // for

			// this is in 0.19.0 style with the extra padding bug
			final MD5Hash md5_of_md5 = MD5Hash.digest(md5outDataBuffer
					.getData());
			returnChecksum = new MD5MD5CRC32FileChecksum(bytesPerCRC,
					crc_per_block, md5_of_md5);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {

			e.printStackTrace();

		} finally {
			in.close();
		} // try

		return returnChecksum;

	}

	/**
	 * This was a work/debug function I was using for testing, will remove soon.
	 * Ignore for most purposes.
	 * 
	 * @param strPath
	 * @throws IOException
	 */
	public void TestHadoopFilesystem(String strPath) throws IOException {

		FileSystem localFs = null;
		// String strPath = "dw_archive.d.jp_1"; //"dw_dbase.dat";
		// String strPath = "foo.zip";
		Path srcPath = null;
		long lFilesize = 0;
		// long l_HDFS_Blocksize = 0;
		long l_LocalBlockSize = 64 * 1024 * 1024;
		int iBlockCount = 0;
		int bytes_per_crc = 512;
		long crc_per_block = l_LocalBlockSize / bytes_per_crc;

		System.out.println(" ------------ ");
		System.out.println("Path: " + strPath);

		// initialize FsShell
		try {
			// init();

			srcPath = new Path(strPath);
			localFs = srcPath.getFileSystem(getConf());

			if (null == localFs) {

				System.out.println("localFs is null! ");

			} else {

				FileStatus f_stats = localFs.getFileStatus(srcPath);

				// l_HDFS_Blocksize = localFs.getDefaultBlockSize();
				lFilesize = localFs.getFileStatus(srcPath).getLen();
				iBlockCount = (int) Math.ceil((double) lFilesize
						/ (double) l_LocalBlockSize);

				System.out.println("getLen == " + f_stats.getLen() + " bytes");
				// System.out.println( "getDefaultBlockSize == " +
				// l_HDFS_Blocksize );
				System.out.println("iBlockCount == " + iBlockCount);

			}

		} catch (RPC.VersionMismatch v) {
			System.err.println("Version Mismatch between client and server"
					+ "... command aborted.");
			// return 0;
		} catch (IOException e) {
			System.err.println("Bad connection to FS. command aborted.");
			// return 0

		}

		if (localFs.getFileStatus(srcPath).isDir()) {

			System.out.println(strPath + " - is a dir");

		} else if (localFs.isFile(srcPath)) {

			String actual_md5 = "31698f57631074eb6e81c34d6c781902";

			DataOutputBuffer md5outDataBuffer = new DataOutputBuffer();

			DataChecksum chksm = DataChecksum.newDataChecksum(
					DataChecksum.CHECKSUM_CRC32, 512);

			InputStream in = null;

			try {

				in = localFs.open(srcPath);
				long lTotalBytesRead = 0;

				for (int x = 0; x < iBlockCount; x++) {

					System.out.println("Block Loop: " + x);

					ByteArrayOutputStream ar_CRC_Bytes = new ByteArrayOutputStream();

					byte crc[] = new byte[4];
					byte buf[] = new byte[512];

					try {

						int bytesRead = in.read(buf);

						while (bytesRead >= 0) {

							lTotalBytesRead += bytesRead;

							if (lTotalBytesRead >= (x + 1) * l_LocalBlockSize) {
								break;
							}

							chksm.reset();
							chksm.update(buf, 0, bytesRead);
							chksm.writeValue(crc, 0, true);
							ar_CRC_Bytes.write(crc);

							bytesRead = in.read(buf);

						} // while

						// System.out.println("Length of CRC byte array: " +
						// ar_CRC_Bytes.size() );

						// ByteArrayInputStream ba = new ByteArrayInputStream(
						// ar_CRC_Bytes.toByteArray() );
						DataInputStream inputStream = new DataInputStream(
								new ByteArrayInputStream(ar_CRC_Bytes
										.toByteArray()));

						// this actually computes one ---- run on the server
						// (DataXceiver) side
						MD5Hash md5_dataxceiver = MD5Hash.digest(inputStream);
						md5_dataxceiver.write(md5outDataBuffer);

						System.out
								.println("md5outDataBuffer.getLength()     : "
										+ md5outDataBuffer.getLength());
						System.out
								.println("md5outDataBuffer.getData().length: "
										+ md5outDataBuffer.getData().length);

					} catch (IOException e) {

						e.printStackTrace();

					} catch (Exception e) {

						e.printStackTrace();

					}

				} // for

				// this is in 0.19.0 style with the extra padding bug
				final MD5Hash md5_of_md5 = MD5Hash.digest(md5outDataBuffer
						.getData());

				// System.out.println("hdfs-chksm: " + actual_md5 );
				System.out.println("md5-of-md5: " + md5_of_md5.toString());

				System.out.println("\n[fin]");

			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception e) {

				e.printStackTrace();

			} finally {
				in.close();
			} // try

		} else {
			// throw new IOException(srcPath.toString() +
			// ": No such file or directory");
		}

		/*
		 * //System.out.println("Num Bytes In Sum: " + chksm.getNumBytesInSum()
		 * ); System.out.println("md5: " + md5 + " - (" + md5.getDigest().length
		 * + " bytes)" );
		 * 
		 * byte pre_hash_zeros[] = new byte[ 16 ];
		 * 
		 * for ( int x = 0; x < 16; x++ ) {
		 * 
		 * pre_hash_zeros[ x ] = 0;
		 * 
		 * }
		 * 
		 * byte f[] = new byte[32]; //Bytes.add( md5.getDigest(), pre_hash_zeros
		 * );
		 * 
		 * System.arraycopy( md5.getDigest(), 0, f, 0, 16 ); System.arraycopy(
		 * pre_hash_zeros, 0, f, 16, 16 );
		 * 
		 * for ( int x = 0; x < 32; x++ ) {
		 * 
		 * System.out.println( f[ x ] );
		 * 
		 * }
		 * 
		 * System.out.println("PreFinal Len: " + f.length + " bytes" );
		 */

	}

}
