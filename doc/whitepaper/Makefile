all: hamake.pdf

hamake.pdf: hamake.tex hamake.bib twofold.png twoforeach.png twoforeachp.png GraphReduction.png sample.xml
	pdflatex hamake.tex
	bibtex hamake
	pdflatex hamake.tex
	pdflatex hamake.tex


%.png: %.dot
	dot -Tpng -o $@ $<

run: hamake.pdf
ifeq ($(OS),Windows_NT)
	cmd.exe /c start hamake.pdf
else
	open hamake.pdf
endif

clean:
	rm -f hamake.pdf hamake.toc hamake.out hamake.log hamake.dvi hamake.aux hamake.bbl hamake.bbg hamake.lof hamake.blg [a-z]*.png
