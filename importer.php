<?php

/**
 * Polish address points PRG dataset XML to SQL importer.
 *
 * A script for converting Polish address points obtained from PRG (Państwowy Rejestr Granic) from XML format into
 * a MySQL table.
 *
 * As of 11/2018, the input files for this script can be downloaded at
 * http://www.gugik.gov.pl/pzgik/dane-bez-oplat/dane-z-panstwowego-rejestru-granic-i-powierzchni-jednostek-podzialow-terytorialnych-kraju-prg.
 *
 * They are the PRG – punkty adresowe dataset in *.GML format.
 */

include("vendor/autoload.php");

use proj4php\Proj4php;
use proj4php\Proj;
use proj4php\Point;

const NUM_TAGS_PER_CHUNK = 100000;
const NUM_ROWS_PER_DB_INSERT = 10000;

/**
 * Check that the script input is OK.
 *
 * @param int $argc
 * @param array $argv
 */
function assert_params_ok(int $argc, array $argv) : void
{
    if (($argc !== 6) || !is_dir($argv[1])) {
        // TODO: Password shouldn't be given on command line.
        echo ""
            . "Usage: php importer.php DIR HOST USERNAME PASSWORD SCHEMA\n"
            . "DIR - directory holding unpacked XML files from PRG, without trailing slash\n"
            . "HOST - MySQL host to connect to (e.g. localhost)\n"
            . "USERNAME - MySQL username\n"
            . "PASSWORD - MySQL password\n"
            . "SCHEMA - MySQL schema to insert data into\n";
        exit(0);
    }
}

/**
 * Returns true if $haystack ends with $needle.
 *
 * @param string $haystack
 * @param string $needle
 * @return bool
 */
function ends_with(string $haystack, string $needle) : bool
{
    $length = strlen($needle);
    if ($length == 0) {
        return true;
    }
    return (substr($haystack, -$length) === $needle);
}

/**
 * Returns an array of filenames ending with a given string in a given directory.
 *
 * @param string $dir
 * @param string $end
 * @return array
 */
function get_files_ending_with(string $dir, string $end) : array
{
   $filenames = [];
   foreach (new DirectoryIterator($dir) as $file) {
       if ($file->isFile() && ends_with($file->getFilename(), $end)) {
           $filenames[] = $file->getFilename();
       }
   }
   return $filenames;
}

/**
 * Divides an XML file with a given filename in a given directory into smaller chunks, putting them in temp directory.
 * The file is expected to contain a list of tags specified by $tag_name param. The function splits the file respecting
 * the boundaries between such tags.
 *
 * @param string $dir
 * @param string $filename
 * @param string $tag_name
 * @param string $tmp_dir
 * @throws Exception if file can't be opened.
 */
function split_file_into_chunks(string $dir, string $filename, string $tag_name, string $tmp_dir) : void
{
    echo "Started splitting file into chunks: [$filename].\n";
    $handle = fopen($dir . '/' . $filename, "r");
    if ($handle) {
        $chunk = [];
        $inside_tag = false;
        $i = 1;
        while (($line = fgets($handle)) !== false) {
            $line = trim($line);
            if (($i % NUM_TAGS_PER_CHUNK === 0) && !$inside_tag) {
                file_put_contents($tmp_dir . '/chunk' . floor($i / NUM_TAGS_PER_CHUNK) . '-' . $filename . '.chunk',
                    '<?xml version="1.0" encoding="UTF-8" ?>' . "\n" . '<root>' . implode("\n", $chunk) . '</root>');
                $chunk = [];
                echo "Processed $i tags.\n";
            }

            if (strpos($line, '<' . $tag_name) === 0) {
                $inside_tag = true;
            }
            if ($inside_tag) {
                $chunk[] = $line;
            }
            if (strpos($line, '</' . $tag_name . '>') === 0) {
                $inside_tag = false;
                $i++;
            }
        }

        file_put_contents($tmp_dir . '/chunk' . (floor($i / NUM_TAGS_PER_CHUNK) + 1) . '-' . $filename . '.chunk',
            '<?xml version="1.0" encoding="UTF-8" ?>' . "\n" . '<root>' . implode("\n", $chunk) . '</root>');

        fclose($handle);
    } else {
        throw new \Exception('Error opening file: [' . $dir . '/' . $filename . '].');
    }
    echo "Ended splitting file into chunks: [$filename].\n";
}

/**
 * Remove namespaced XML tags and attributes from the files (simplexml_load_file() fails if they are there).
 *
 * @param string $tmp_dir
 * @param string $filename
 */
function remove_namespaced_xml(string $tmp_dir, string $filename) : void
{
    $content = file_get_contents($tmp_dir . '/' . $filename);
    $content = str_replace("gml:", "", $content);
    $content = str_replace("prg-ad:", "", $content);
    $content = str_replace("bt:", "bt", $content);
    $content = str_replace("xsi:", "xsi", $content);
    $content = str_replace("xlink:", "xlink", $content);
    file_put_contents($tmp_dir . '/' . $filename . '.no_namespaced_xml', $content);
    echo "Removed namespaced XML from file: [$filename].\n";
}

/**
 * Returns a MySQL connection.
 *
 * @param array $argv
 * @return mysqli
 */
function create_mysql_connection(array $argv)
{
    $conn = mysqli_connect($argv[2], $argv[3], $argv[4], $argv[5]);
    $conn->set_charset('utf8mb4');
    $conn->query('SET collation_connection = utf8mb4_unicode_520_ci');
    return $conn;
}

/**
 * Imports a processed PRG chunk file into MySQL table.
 *
 * @param string $tmp_dir
 * @param string $filename
 * @param $conn
 * @param Proj4php $proj4
 * @param Proj $projPl
 * @param Proj $projLatLon
 */
function import_file_into_mysql(string $tmp_dir, string $filename, $conn,
                                Proj4php $proj4, Proj $projPl, Proj $projLatLon) : void
{
    echo "Importing file into MySQL: [$filename].\n";
    $base_insert = "INSERT INTO x (kod_pocztowy, miejscowosc, ulica, numer, lat, lon) VALUES ";
    $xml = simplexml_load_file($tmp_dir . '/' . $filename);
    $i = 0;
    $sql = $base_insert;
    foreach ($xml->xpath('//PRG_PunktAdresowy') as $row) {
        $kod_pocztowy = mysqli_real_escape_string($conn, $row->kodPocztowy);
        $miejscowosc = mysqli_real_escape_string($conn, $row->jednostkaAdmnistracyjna[3]);
        $ulica = mysqli_real_escape_string($conn, $row->ulica);
        $numer = mysqli_real_escape_string($conn, $row->numerPorzadkowy);
        $xy = explode(' ', $row->pozycja->Point->pos);
        $y = mysqli_real_escape_string($conn, $xy[0]);
        $x = mysqli_real_escape_string($conn, $xy[1]);
        $pointPl = new Point($x, $y, $projPl);
        $pointLatLon = $proj4->transform($projLatLon, $pointPl);

        $sql = $sql . "('" . $kod_pocztowy . "','" . $miejscowosc . "','" . $ulica . "','" . $numer . "',"
            . $pointLatLon->y . "," . $pointLatLon->x . "),";
        $i++;

        if ($i % NUM_ROWS_PER_DB_INSERT === 0) {
            $result = mysqli_query($conn, rtrim($sql, ',') . ';');
            if (!empty($result)) {
                echo "$i rows inserted.\n";
            } else {
                $error_message = mysqli_error($conn) . "\n";
                echo $error_message;
            }
            $sql = $base_insert;
        }
    }
    if ($i % NUM_ROWS_PER_DB_INSERT !== 0) {
        $result = mysqli_query($conn, rtrim($sql, ',') . ';');
        if (!empty($result)) {
            echo "$i rows inserted.\n";
        } else {
            $error_message = mysqli_error($conn) . "\n";
            echo $error_message;
        }
    }
}

/**
 * Removes the temp directory and its contents.
 *
 * @param string $tmp_dir
 * @throws Exception
 */
function clean_up(string $tmp_dir)
{
    $chunk_filenames = get_files_ending_with($tmp_dir, '.chunk');
    foreach ($chunk_filenames as $filename) {
        $is_unlink_successful = unlink($tmp_dir . '/' . $filename);
        if (!$is_unlink_successful) {
            throw new \Exception('File unlink unsuccessful: [' . $tmp_dir . '/' . $filename . '].');
        }
    }

    $no_namespaced_xml_filenames = get_files_ending_with($tmp_dir, '.no_namespaced_xml');
    foreach ($no_namespaced_xml_filenames as $filename) {
        $is_unlink_successful = unlink($tmp_dir . '/' . $filename);
        if (!$is_unlink_successful) {
            throw new \Exception('File unlink unsuccessful: [' . $tmp_dir . '/' . $filename . '].');
        }
    }

    $is_tmp_dir_removed = rmdir($tmp_dir);
    if (!$is_tmp_dir_removed) {
        throw new \Exception('Temp dir could not be deleted: [' . $tmp_dir . '].');
    }
}

function main(int $argc, array $argv)
{
    assert_params_ok($argc, $argv);
    $dir = $argv[1];
    $now = new \DateTime();
    $tmp_dir = $dir . '/tmp-' . $now->format('Y-m-d-H-i-s');
    $is_tmp_dir_created = mkdir($tmp_dir);
    if (!$is_tmp_dir_created) {
        throw new \Exception('Could not create a temporary directory: [' . $tmp_dir . '].');
    }

    $input_filenames = get_files_ending_with($dir, '.xml');
    foreach ($input_filenames as $filename) {
        split_file_into_chunks($dir, $filename, 'prg-ad:PRG_PunktAdresowy', $tmp_dir);
    }

    $chunk_filenames = get_files_ending_with($tmp_dir, '.chunk');
    foreach ($chunk_filenames as $filename) {
        remove_namespaced_xml($tmp_dir, $filename);
    }

    $conn = create_mysql_connection($argv);
    $proj4 = new Proj4php();
    $projPl = new Proj('EPSG:2180', $proj4); // Projection which the PRG files use.
    $projLatLon = new Proj('EPSG:4326', $proj4); // Projection using standard latitude & longitude.
    $no_namespaced_xml_filenames = get_files_ending_with($tmp_dir, '.no_namespaced_xml');
    foreach ($no_namespaced_xml_filenames as $filename) {
        import_file_into_mysql($tmp_dir, $filename, $conn, $proj4, $projPl, $projLatLon);
    }

    clean_up($tmp_dir);
    echo "Script completed successfully.\n";
}

main($argc, $argv);
