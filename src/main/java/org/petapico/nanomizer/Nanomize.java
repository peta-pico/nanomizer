package org.petapico.nanomizer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.activation.MimetypesFileTypeMap;

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
	private RDFFormat format;

	private Map<String,Set<String>> fwLinkMap = new HashMap<>();
	private Map<String,Set<String>> bwLinkMap = new HashMap<>();

	boolean mergeComplete = false;

	private Map<String,String> mainNodeMap = new HashMap<>();
	private Map<String,Set<String>> sideNodeMap = new HashMap<>();

	public Nanomize(File file) {
		this.file = file;
		String n = file.getName();
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
		InputStream in = new FileInputStream(file);
		try {
			readData(in);
		} finally {
			in.close();
		}
		while (!mergeComplete) {
			mergeNodes();
		}
		showResults();
	}

	private void readData(InputStream in) throws OpenRDFException, IOException {
		RDFParser p = NanopubUtils.getParser(format);
		p.setRDFHandler(new RDFHandlerBase() {

			@Override
			public void handleStatement(Statement st) throws RDFHandlerException {
				if (st.getSubject() instanceof BNode) {
					throw new RuntimeException("Unexpected blank node encountered");
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
			bwLinkMap.put(node, EMPTYSET); // so we get here at most once per node
			String m = bwLinks.iterator().next();
			System.out.println("Merge " + node + " into " + m);
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
			if (fwLinkMap.containsKey(node)) {
				if (!fwLinkMap.containsKey(m)) fwLinkMap.put(m, new HashSet<String>());
				fwLinkMap.get(m).addAll(fwLinkMap.get(node));
				fwLinkMap.get(m).remove(m); // remove main node as its own forward link, if present
				fwLinkMap.remove(node);
			}
			mergeHappened = true;
		}
		if (!mergeHappened) mergeComplete = true;
	}

	private void showResults() {
		System.out.println("FORWARD LINKS:");
		for (String node : fwLinkMap.keySet()) {
			System.out.println(node + ": " + fwLinkMap.get(node).size());
		}
		System.out.println("BACKWARD LINKS:");
		for (String node : bwLinkMap.keySet()) {
			System.out.println(node + ": " + bwLinkMap.get(node).size());
		}
	}

}
