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

public class Nanomize {

	public static void main(String[] args) throws OpenRDFException, IOException {
		for (String filename : args) {
			Nanomize r = new Nanomize(new File(filename));
			r.run();
		}
	}

	private static final MimetypesFileTypeMap mimeMap = new MimetypesFileTypeMap();

	private File file;
	private RDFFormat format;

	private Map<String,Set<String>> fwLinkMap = new HashMap<String, Set<String>>();
	private Map<String,Set<String>> bwLinkMap = new HashMap<String, Set<String>>();

	public Nanomize(File file) {
		this.file = file;
		String n = file.getName();
		format = Rio.getParserFormatForMIMEType(mimeMap.getContentType(n));
		if (format == null || !format.supportsContexts()) {
			format = Rio.getParserFormatForFileName(n, RDFFormat.NQUADS);
		}
		if (!format.supportsContexts()) {
			format = RDFFormat.NQUADS;
		}
	}

	public void run() throws OpenRDFException, IOException {
		System.out.println("Processing file: " + file);
		InputStream in = new FileInputStream(file);
		try {
			processStream(in);
		} finally {
			in.close();
		}
		showResults();
	}

	private void processStream(InputStream in) throws OpenRDFException, IOException {
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
		if (!fwLinkMap.containsKey(subj)) fwLinkMap.put(subj, new HashSet<String>());
		fwLinkMap.get(subj).add(obj);
		if (!bwLinkMap.containsKey(obj)) bwLinkMap.put(obj, new HashSet<String>());
		bwLinkMap.get(obj).add(subj);
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
