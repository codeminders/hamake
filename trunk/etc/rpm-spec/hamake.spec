Name: hamake
Summary: Hamake is a lightweight utility and workflow engine for Hadoop.
Version: #@hamake.version@
Release: #@release.number@
Group: Applications/MapReduce
License: ASL 2.0
Requires: hadoop
Requires: hadoop-pig >= 0.5.0
URL: http://code.google.com/p/hamake
%description
Hamake is a lightweight utility and workflow engine for Hadoop. Hamake helps to organize your Hadoop Map Reduce jobs, Pig script and local programs in a workflow and launch them based on dataflow principles - your tasks will be executed$

%install
mkdir -p $RPM_BUILD_ROOT%{_libdir}
cp -r ../usr/lib/%{name} $RPM_BUILD_ROOT%{_libdir}

%clean
rm -drf %{_libdir}/%{name}

%post
sed -i 's/#export\sHAMAKE_HOME=/export HAMAKE_HOME=\/usr\/lib\/hamake/' %{_libdir}/%{name}/examples/bin/start-class-size-example.sh

%files
%defattr(-,root,root,-)
%{_libdir}/%{name}/%{name}-%{version}-%{release}.jar
%{_libdir}/%{name}/examples/hamake-examples-%{version}-%{release}.jar
%{_libdir}/%{name}/examples/hamakefiles/class-size.xml
%{_libdir}/%{name}/examples/hamakefiles/class-size-s3.xml
%{_libdir}/%{name}/examples/scripts/median.pig
%{_libdir}/%{name}/examples/bin/start-class-size-example.sh
%{_libdir}/%{name}/README.txt
%attr(-,hamake,hamake) %{_libdir}/%{name}
%attr(0755,hamake,hamake) %{_libdir}/%{name}/examples/bin/*

%changelog
* Tue Apr 27 2010 Bondar Alex <abondar@codeminders.com> 1.0.0
- Initial version
