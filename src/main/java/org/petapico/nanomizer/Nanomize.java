package org.petapico.nanomizer;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.activation.MimetypesFileTypeMap;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.nanopub.NanopubUtils;
import org.openrdf.OpenRDFException;
import org.openrdf.model.BNode;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.RDFHandlerBase;

import com.google.common.collect.ImmutableSet;

public class Nanomize {

	public static void main(String[] args) throws OpenRDFException, IOException {
		for (String filename : args) {
			Nanomize r = new Nanomize(new File(filename));
			r.run();
		}
	}

	private static final MimetypesFileTypeMap mimeMap = new MimetypesFileTypeMap();
	private static final Set<String> EMPTYSET = ImmutableSet.of();

	private File file;
	private String compressionFormat = null;
	private RDFFormat format;

	private Map<String,Set<String>> fwLinkMap = new HashMap<>();
	private Map<String,Set<String>> bwLinkMap = new HashMap<>();

	boolean mergeComplete = false;

	private Map<String,String> mainNodeMap = new HashMap<>();
	private Map<String,Set<String>> sideNodeMap = new HashMap<>();

	private int maxTripleCount = 0;
	private Map<String,Integer> tripleCount = new HashMap<>();

	private boolean blankNodesEncountered = false;

	public Nanomize(File file) {
		this.file = file;
		String n = file.getName();
		if (n.endsWith(".gz")) {
			compressionFormat = CompressorStreamFactory.GZIP;
			n = n.replaceFirst("\\.gz$", "");
		} else if (n.endsWith(".bz2")) {
			compressionFormat = CompressorStreamFactory.BZIP2;
			n = n.replaceFirst("\\.bz2$", "");
		}
		format = Rio.getParserFormatForMIMEType(mimeMap.getContentType(n));
		if (format == null || !format.supportsContexts()) {
			format = Rio.getParserFormatForFileName(n, RDFFormat.NTRIPLES);
		}
		if (!format.supportsContexts()) {
			format = RDFFormat.NTRIPLES;
		}
	}

	public void run() throws OpenRDFException, IOException {
		System.out.println("Processing file: " + file);
		InputStream in = getInputStream();
		try {
			readData(in);
		} finally {
			in.close();
		}
		while (!mergeComplete) {
			mergeNodes();
		}
		in = getInputStream();
		try {
			makePartition(in);
		} finally {
			in.close();
		}
		showResults();
	}

	private InputStream getInputStream() throws IOException {
		InputStream in;
		try {
			if (compressionFormat == null) {
				in = new FileInputStream(file);
			} else {
				in = new CompressorStreamFactory().createCompressorInputStream(compressionFormat, new FileInputStream(file));
			}
		} catch (CompressorException ex) {
			in = new FileInputStream(file);
		}
		return new BufferedInputStream(in);
	}

	private void readData(InputStream in) throws OpenRDFException, IOException {
		RDFParser p = NanopubUtils.getParser(format);
		p.setRDFHandler(new RDFHandlerBase() {

			@Override
			public void handleStatement(Statement st) throws RDFHandlerException {
				if (st.getSubject() instanceof BNode) {
					blankNodesEncountered = true;
					return;
				}
				String s = st.getSubject().stringValue();
				if (!(st.getObject() instanceof URI)) return;
				String o = st.getObject().stringValue();
				addLink(s, o);
			}

		});
		p.parse(in, "");
	}

	private void addLink(String subj, String obj) {
		if (subj.equals(obj)) return;
		if (!fwLinkMap.containsKey(subj)) fwLinkMap.put(subj, new HashSet<String>());
		fwLinkMap.get(subj).add(obj);
		if (!bwLinkMap.containsKey(obj)) bwLinkMap.put(obj, new HashSet<String>());
		bwLinkMap.get(obj).add(subj);
	}

	private void mergeNodes() {
		System.out.println("RUN MERGE...");
		boolean mergeHappened = false;
		for (String node : bwLinkMap.keySet()) {
			Set<String> bwLinks = bwLinkMap.get(node);
			if (bwLinks.size() != 1) continue;
			String m = bwLinks.iterator().next();
			System.out.println("Merge " + node + " into " + m);
			if (fwLinkMap.containsKey(node)) {
				for (String s : fwLinkMap.get(node)) {
					if (s.equals(m)) continue;
					Set<String> bw = bwLinkMap.get(getMainNode(s));
					bw.remove(node);
					bw.add(m);
				}
				if (!fwLinkMap.containsKey(m)) fwLinkMap.put(m, new HashSet<String>());
				fwLinkMap.get(m).addAll(fwLinkMap.get(node));
				fwLinkMap.get(m).remove(m); // remove main node as its own forward link, if present
				fwLinkMap.remove(node);
			}
			bwLinkMap.put(node, EMPTYSET); // so we don't process the same node again
			mainNodeMap.put(node, m);
			if (!sideNodeMap.containsKey(m)) sideNodeMap.put(m, new HashSet<String>());
			Set<String> sideNodes = sideNodeMap.get(m);
			sideNodes.add(node);
			if (sideNodeMap.containsKey(node)) {
				for (String s : sideNodeMap.get(node)) {
					if (s.equals(m)) continue;
					mainNodeMap.put(s, m);
				}
				sideNodes.addAll(sideNodeMap.get(node));
				sideNodes.remove(m); // remove main node as its own side node, if present
				sideNodeMap.remove(node);
			}
			mergeHappened = true;
		}
		if (!mergeHappened) mergeComplete = true;
	}

	private String getMainNode(String node) {
		if (mainNodeMap.containsKey(node)) return mainNodeMap.get(node);
		return node;
	}

	private void makePartition(InputStream in) throws OpenRDFException, IOException {
		RDFParser p = NanopubUtils.getParser(format);
		p.setRDFHandler(new RDFHandlerBase() {

			@Override
			public void handleStatement(Statement st) throws RDFHandlerException {
				String s = st.getSubject().stringValue();
				if (mainNodeMap.containsKey(s)) s = mainNodeMap.get(s);
				if (!tripleCount.containsKey(s)) tripleCount.put(s, 0);
				int c = tripleCount.get(s) + 1;
				tripleCount.put(s, c);
				if (c > maxTripleCount) maxTripleCount = c;
			}

		});
		p.parse(in, "");
	}

	private void showResults() {
		System.out.println("MAIN NODES:");
		for (String node : sideNodeMap.keySet()) {
			System.out.println(node + ":");
			for (String side : sideNodeMap.get(node)) {
				System.out.println("  " + side);
			}
		}

		System.out.println("DISTRIBUTION OF PARTITION SIZE (NUMBER OF TRIPLES):");
		int[] tripleCountDist = new int[maxTripleCount+1];
		for (String mainNode : tripleCount.keySet()) {
			tripleCountDist[tripleCount.get(mainNode)]++;
		}
		for (int i = 1 ; i <= maxTripleCount ; i++) {
			if (tripleCountDist[i] == 0) continue;
			System.out.println(i + ": " + tripleCountDist[i]);
		}

		System.out.println("DISTRIBUTION OF PARTITION SIZE (NUMBER OF NODES):");
		int maxSideNodes = 1;
		for (Set<String> s : sideNodeMap.values()) {
			if (s.size() + 1 > maxSideNodes) maxSideNodes = s.size() + 1;
		}
		int[] nodeCountDist = new int[maxSideNodes+1];
		for (String mainNode : tripleCount.keySet()) {
			if (sideNodeMap.containsKey(mainNode)) {
				nodeCountDist[sideNodeMap.get(mainNode).size()+1]++;
			} else {
				nodeCountDist[1]++;
			}
		}
		for (int i = 1 ; i <= maxSideNodes ; i++) {
			if (nodeCountDist[i] == 0) continue;
			System.out.println(i + ": " + nodeCountDist[i]);
		}

		if (blankNodesEncountered) {
			System.out.println("WARNING: blank nodes encountered");
		}
	}

}
