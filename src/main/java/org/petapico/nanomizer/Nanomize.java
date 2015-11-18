package org.petapico.nanomizer;

import java.io.File;

public class Nanomize {

	public static void main(String[] args) {
		for (String filename : args) {
			Nanomize r = new Nanomize(new File(filename));
			r.run();
		}
	}

	private File file;

	public Nanomize(File file) {
		this.file = file;
	}

	public void run() {
		System.out.println("Processing file: " + file);
	}

}
