/*
 * This file is part of Hadoop-Gpl-Compression.
 *
 * Hadoop-Gpl-Compression is free software: you can redistribute it
 * and/or modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Hadoop-Gpl-Compression is distributed in the hope that it will be
 * useful, but WITHOUT ANY WARRANTY; without even the implied warranty
 * of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Hadoop-Gpl-Compression.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

package com.hadoop.compression.lzo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;
import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.Decompressor;

/**
 * Test the LzopInputStream, making sure we get the same bytes when reading a compressed file through
 * it as when we read the corresponding uncompressed file through a standard input stream.
 */
public class TestLzopInputStream extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestLzopInputStream.class);

  private String inputDataPath;

  // Filenames of various sizes to read in and verify.
  private final String bigFile = "100000.txt";
  private final String mediumFile = "1000.txt";
  private final String smallFile = "100.txt";
  private final String emptyFile = "0.txt";

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    inputDataPath = System.getProperty("test.build.data", "data");
  }

  /**
   * Test against a 100,000 line file with multiple LZO blocks.
   */
  public void testBigFile() throws NoSuchAlgorithmException, IOException,
  InterruptedException {    
    runTest(bigFile);
  }

  /**
   * Test against a 1,000 line file with a single LZO block.
   */
  public void testMediumFile() throws NoSuchAlgorithmException, IOException,
  InterruptedException {    
    runTest(mediumFile);
  }

  /**
   * Test against a 100 line file that is a single LZO block.  Moreover, this
   * file compresses to a size larger than its uncompressed size, so the LZO 
   * format mandates that it's stored differently on disk.  Instead of the usual block
   * format, which is 
   * uncompressed size | compressed size | uncompressed checksum | compressed checksum | compressed data
   * in this case the block gets stored as
   * uncompressed size | uncompressed size | uncompressed checksum | uncompressed data
   * with no additional checksum.  Thus the read has to follow a slightly different codepath.
   */
  public void testSmallFile() throws NoSuchAlgorithmException, IOException,
  InterruptedException {    
    runTest(smallFile);
  }

  /**
   * Test against a 0 line file.
   */
  public void testEmptyFile() throws NoSuchAlgorithmException, IOException,
  InterruptedException {
    runTest(emptyFile);
  }

  /**
   * Test that reading an lzo-compressed file produces the same lines as reading the equivalent
   * flat file.  The test opens both the compressed and flat file, successively reading each
   * line by line and comparing.
   */
  private void runTest(String filename) throws IOException,
  NoSuchAlgorithmException, InterruptedException {

    if (!GPLNativeCodeLoader.isNativeCodeLoaded()) {
      LOG.warn("Cannot run this test without the native lzo libraries");
      return;
    }

    // Assumes the flat file is at filename, and the compressed version is filename.lzo
    File textFile = new File(inputDataPath, filename);
    File lzoFile = new File(inputDataPath, filename + new LzopCodec().getDefaultExtension());
    LOG.info("Comparing files " + textFile + " and " + lzoFile);

    // Set up the text file reader.
    BufferedReader textBr = new BufferedReader(new InputStreamReader(new FileInputStream(textFile.getAbsolutePath())));
    // Set up the LZO reader.
    int lzoBufferSize = 256 * 1024;
    LzopDecompressor lzoDecompressor = new LzopDecompressor(lzoBufferSize);
    LzopInputStream lzoIn = new LzopInputStream(new FileInputStream(lzoFile.getAbsolutePath()), lzoDecompressor, lzoBufferSize);
    BufferedReader lzoBr = new BufferedReader(new InputStreamReader(lzoIn));

    // Now read line by line and compare.
    String textLine;
    String lzoLine;
    int line = 0;
    while ((textLine = textBr.readLine()) != null) {
      line++;
      lzoLine = lzoBr.readLine();
      if (!lzoLine.equals(textLine)) {
        LOG.error("LZO decoding mismatch on line " + line + " of file " + filename);
        LOG.error("Text line: [" + textLine + "], which has length " + textLine.length());
        LOG.error("LZO line: [" + lzoLine + "], which has length " + lzoLine.length());
      }
      assertEquals(lzoLine, textLine);
    }
    // Verify that the lzo file is also exhausted at this point.
    assertNull(lzoBr.readLine());
    
    textBr.close();
    lzoBr.close();
  }

  /**
   * Test case that uses the codec like IFile.Reader does, calling close()
   * on the input stream *after* returning the decompressor to the
   * codec pool. This is a test that illustrates MAPREDUCE-2258
   */
  public void testMultiThreadIFileLike() throws Exception {
    if (!GPLNativeCodeLoader.isNativeCodeLoaded()) {
      LOG.warn("Cannot run this test without the native lzo libraries");
      return;
    }

    Configuration conf = new Configuration();
    final LzopCodec codec = new LzopCodec();
    codec.setConf(conf);

    File uncompressedFile = new File(inputDataPath,
      bigFile);
    final long uncompressedSize = uncompressedFile.length();

    ExecutorService exec = Executors.newFixedThreadPool(30);
    ArrayList<Future<Void>> futures = new ArrayList<Future<Void>>();
    for (int i = 0; i < 100; i++) {
      futures.add(exec.submit(new Callable<Void>() {
        public Void call() throws Exception {
          for (int j = 0; j < 100; j++) {
            long numBytesRead = exerciseCodecFromPool(codec);
            assertEquals(uncompressedSize, numBytesRead);
          }
          return null;
        }
      }));
    }

    for (Future<Void> f : futures) {
      f.get();
    }
    exec.shutdown();
  }

  private long exerciseCodecFromPool(LzopCodec codec) throws Exception  {
    File lzoFile = new File(inputDataPath,
      bigFile + codec.getDefaultExtension());
    InputStream is = new FileInputStream(lzoFile);

    Decompressor decomp = CodecPool.getDecompressor(codec);
    InputStream lzoIn = codec.createInputStream(is, decomp);

    byte[] readData = new byte[1024];
    long read = 0;
    int n;
    while ((n = lzoIn.read(readData)) > 0) {
      read += n;
    }

    // Close the codec up in the same way that IFile.Reader.close does -
    // this is probably wrong - see MAPREDUCE-2258
    Thread.sleep(1);
    decomp.reset();
    Thread.sleep(1);
    CodecPool.returnDecompressor(decomp);
    Thread.sleep(1);
    lzoIn.close();

    return read;
  }
}
