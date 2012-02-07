all: hamake.pdf

hamake.pdf: hamake.tex hamake.bib SimilarityAlgDAG.eps twofoldp.eps twoforeachp.eps GraphReduction.eps sample.xml
	pdflatex hamake.tex
	bibtex hamake
	pdflatex hamake.tex
	pdflatex hamake.tex

run: hamake.pdf
ifeq ($(OS),Windows_NT)
	cmd.exe /c start hamake.pdf
else
	open hamake.pdf
endif

clean:
	rm -f hamake.pdf hamake.toc hamake.out hamake.log hamake.dvi hamake.aux hamake.bbl hamake.bbg hamake.lof hamake.blg *eps-converted-to.pdf
