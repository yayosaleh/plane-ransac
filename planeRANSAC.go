package main

import (
	"bufio"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"time"
)

//**INFRASTRUCTURE**//

//STRUCTURES//

type Point3D struct {
	X float64
	Y float64
	Z float64
}
type Plane3D struct {
	A float64
	B float64
	C float64
	D float64
}
type Plane3DwSupport struct {
	Plane3D
	SupportSize int
}

//READING AND SAVING FILES//

// Reads an XYZ file and returns a slice of Point3D
func ReadXYZ(filename string) []Point3D {

	//Open file

	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("Error occured, could not read points from file!")
		panic(err)
	}
	defer file.Close()

	//Read each line of file, store coordinates in Point3D instances, and append point to slice

	scanner := bufio.NewScanner(file)
	var points []Point3D
	scanner.Scan() //skip the first line (containing header)

	for scanner.Scan() {
		line := scanner.Text()         //read line into a string
		coords := strings.Fields(line) //split string by white space
		x, _ := strconv.ParseFloat(coords[0], 64)
		y, _ := strconv.ParseFloat(coords[1], 64)
		z, _ := strconv.ParseFloat(coords[2], 64)
		points = append(points, Point3D{X: x, Y: y, Z: z})
	}

	return points
}

// Saves a slice of Point3D into an XYZ file
func SaveXYZ(filename string, points []Point3D) {

	//Attempt to create file

	file, err := os.Create(filename)
	if err != nil {
		fmt.Println("Error occured, could not save points to file!")
		panic(err)
	}
	defer file.Close()

	//Write each point's coordinates to a new line in the file
	//Is '\n' cross-platform compatible?

	_, err = file.WriteString("X Y Z\n")
	if err != nil {
		panic(err)
	}

	for _, point := range points {
		line := fmt.Sprintf("%v %v %v\n", point.X, point.Y, point.Z) //using %f causes precision loss
		_, err = file.WriteString(line)
		if err != nil {
			panic(err)
		}
	}
}

//PLANE FUNCTIONS & METHODS//

// Helper function used by GetPlane()
func crossProduct(A, B []float64) []float64 {
	return []float64{
		A[1]*B[2] - A[2]*B[1],
		A[2]*B[0] - A[0]*B[2],
		A[0]*B[1] - A[1]*B[0],
	}
}

// Computes the plane defined by a set of 3 points
func GetPlane(points [3]Point3D) Plane3D {

	//Assuming desired from of plane equaiton is: Ax + By + Cz = D

	p1, p2, p3 := points[0], points[1], points[2]
	v1 := []float64{p2.X - p1.X, p2.Y - p1.Y, p2.Z - p1.Z} //vector 1
	v2 := []float64{p3.X - p1.X, p3.Y - p1.Y, p3.Z - p1.Z} //vector 2
	N := crossProduct(v1, v2)                              //normal vector

	result := Plane3D{
		A: N[0],
		B: N[1],
		C: N[2],
	}
	result.D = (result.A * p1.X) + (result.B * p1.Y) + (result.C * p1.Z)
	return result
}

// (Method) Computes distance between a plane and a given point
func (pl *Plane3D) GetDistance(pt Point3D) float64 {
	return math.Abs((pl.A*pt.X)+(pl.B*pt.Y)+(pl.C*pt.Z)-pl.D) / math.Sqrt(math.Pow(pl.A, 2)+math.Pow(pl.B, 2)+math.Pow(pl.C, 2))
}

//RANSAC FUNCTIONS//

// Computes the number of required RANSAC iterations
func GetNumberOfIterations(confidence, percentageOfPointsOnPlane float64) int {
	//Handling case where arguments are passed as percentages
	if confidence > 1 {
		confidence = confidence / 100
	}
	if percentageOfPointsOnPlane > 1 {
		percentageOfPointsOnPlane = percentageOfPointsOnPlane / 100
	}

	//The value we return is the number of random triplets we must pick from the cloud to find the dom. plane ( = number of req. iterations )
	return int(math.Log10(1-confidence) / math.Log10(1-math.Pow(percentageOfPointsOnPlane, 3)))
}

// Computes the support of a plane in a set of points
func GetSupport(plane Plane3D, points []Point3D, eps float64) Plane3DwSupport {

	support := 0

	//"Count the number of points that are at a distance LESS than eps (ε)..."

	for _, pt := range points {
		if plane.GetDistance(pt) < eps {
			support++
		}
	}

	return Plane3DwSupport{
		Plane3D:     plane,
		SupportSize: support,
	}
}

// Extracts the points that supports the given plane and returns them as a slice of points
func GetSupportingPoints(plane Plane3D, points []Point3D, eps float64) []Point3D {

	inliers := []Point3D{}

	for _, pt := range points {
		if plane.GetDistance(pt) < eps {
			inliers = append(inliers, pt)
		}
	}

	return inliers
}

// Creates a new slice of points in which all points belonging to the plane have been removed
func RemovePlane(plane Plane3D, points []Point3D, eps float64) []Point3D {

	remainder := []Point3D{}

	for _, pt := range points {
		if plane.GetDistance(pt) >= eps {
			remainder = append(remainder, pt)
		}
	}

	return remainder
}

//**PIPELINE**//

//Note: every stage of the pipeline runs concurrently (i.e., on separate "threads"); however, only SupportingPointFinder() runs in parallel (this is a heavy computation that benefits from parallelism)

// Randomly selects a point from the provided slice of Point3D (point cloud) -> transmits instances of Point3D through output channel
func RandomPointGenerator(points []Point3D, stop <-chan bool) <-chan Point3D {

	/*
		Channels:
		- This function returns a stream (receive-only channel) through which random points are transmitted;
		  by writing <-chan _ as the return type, it means the CALLER of this function can only receive
		  values from the channel; however, the function itself can send values to this channel!
		- Unlike the remaining stages of the pipeline, this function is given a receive-only control channel, stop,
		  which is populated by TakeN(). This is necessary since the termination of this stage depends on a later
		  stage in the pipeline, rather than a previous one.
		- When stop is sent a value, the output channel of this function, randomPointStream, is closed, and initiates
		  the termination cascade of the pipeline (i.e., TripletGenerator terminates when randomPointStream is closed, and so on...)


		Random point generation:

		Need to produce random ints in the range [0, n), where n is the size of the point cloud,
		in order to index the provided slice.

		We want different random numbers everytime we run, so we'll set the seed based on the current time;
		however; when trying to optimize the number of threads, we won't set the seed so the random
		points generated are a control variable.

		The select statement below will block until 'stop' has been sent a value, causing the
		goroutine to terminate, OR until randomPointStream's value has been received, at which point we
		can send a new random point.
	*/

	randomPointStream := make(chan Point3D)
	//rand.Seed(time.Now().UnixNano())
	go func() {
		defer close(randomPointStream) //crucial since closing this channel is how we terminate the entire pipeline!
		n := len(points)
		randomIndex := rand.Intn(n)
		for {
			select {
			case <-stop:
				return
			case randomPointStream <- points[randomIndex]:
				randomIndex = rand.Intn(n)
			}
		}
	}()
	return randomPointStream
}

// (Helper) returns true if target point is contained within array of points, false otherwise
func ContainsPoint(target Point3D, points [3]Point3D) bool {
	for _, p := range points {
		if p == target {
			return true
		}
	}
	return false
}

// Reads Point3D instances from input channel and accumulates 3 points -> transmits arrays of Point3D (composed of three points) through output channel
func TripletGenerator(randomPointStream <-chan Point3D) <-chan [3]Point3D {
	tripletStream := make(chan [3]Point3D)
	go func() {
		defer close(tripletStream)
		for {
			//1) Gathers three points from input stream into array
			i := 0
			triplet := [3]Point3D{}
			for i < 3 {
				randomPoint, open := <-randomPointStream //blocks until randomPointStream has a new value to send, OR is closed
				if open == false {                       //if randomPointStream is closed, we terminate this goroutine
					return
				}
				if !ContainsPoint(randomPoint, triplet) {
					triplet[i] = randomPoint
					i++
				}
			}
			//2) Sends array to output stream
			tripletStream <- triplet //blocks until tripletStream can be sent another value (i.e., it is empty)
		}
	}()
	return tripletStream
}

// Reads array of Point3D from its input channel -> retransmits array of Point3D through its output channel. Automatically stops the pipeline after having recieved n slices.
func TakeN(n int, tripletStream <-chan [3]Point3D, stop chan<- bool) <-chan [3]Point3D {
	//Here, stop is the same channel that is passed to RandomPointGenerator()
	nTripletStream := make(chan [3]Point3D)
	go func() {
		defer close(nTripletStream)
		numTriplets := 0
		for numTriplets < n {
			triplet := <-tripletStream //blocks until tripletStream is sent a new value
			nTripletStream <- triplet  //blocks until nTripletStream can recieve a new value (i.e., until it is empty)
			numTriplets++
		}
		stop <- true //initiates termination cascade of generators (randomGenerator terminates and then forces tripletStream to terminate, etc.)
	}()
	return nTripletStream
}

// Reads array of Point3D from its input channel and computes the plane defined by those points -> transmits Plane3D instances through output channel
func PlaneEstimator(nTripletStream <-chan [3]Point3D) <-chan Plane3D {
	//Only passed a single channel so there is no need to use select; instead, we terminate contained goroutine when nTripletStream is closed
	planeStream := make(chan Plane3D)
	go func() {
		defer close(planeStream)
		//The below range blocks until nTripletStream has a new value to send, and breaks when nTripletStream is closed!
		//This style is equivelant to a for {} containing a manual check if the input stream is closed as a means of breaking!
		for triplet := range nTripletStream {
			plane := GetPlane(triplet)
			planeStream <- plane //blocks until planeStream can be sent a new value (i.e., when it is empty)
		}
	}()
	return planeStream
}

// Counts the number of points in the given slice of Point3D (point cloud) that support the given Plane3D instance -> transmits Plane3DwSupport instances through its output channel
func SupportingPointFinder(plane Plane3D, points []Point3D, eps float64) <-chan Plane3DwSupport {
	planeSupportChan := make(chan Plane3DwSupport)
	go func() {
		defer close(planeSupportChan)
		planeSupport := GetSupport(plane, points, eps)
		planeSupportChan <- planeSupport //planeSupportChan is only ever sent ONE Plane3DwSupport instance before being closed!
	}()
	return planeSupportChan
}

// (Helper) Reads Plane3D instances from input channel and launches parallel instances of SupportingPointFinder() -> returns a slice of channels where each chan. transmits one Plane3DwSupport instance
func FanOut(points []Point3D, eps float64, planeStream <-chan Plane3D) []<-chan Plane3DwSupport {
	planeSupportChans := []<-chan Plane3DwSupport{}
	for plane := range planeStream { //range blocks until planeStream has a new value to send, and breaks when planeStream is closed
		planeSupportChans = append(planeSupportChans, SupportingPointFinder(plane, points, eps))
	}
	return planeSupportChans
}

// Multiplexes slice of Plane3DwSupport channels into a single Plane3DwSupport output channel
func FanIn(planeSupportChans []<-chan Plane3DwSupport) <-chan Plane3DwSupport {
	planeSupportStream := make(chan Plane3DwSupport)
	go func() {
		defer close(planeSupportStream)
		//Loop blocks until every dominant plane candidate has been processed on its own "thread"
		for _, planeSupportChan := range planeSupportChans {
			planeSupport := <-planeSupportChan //blocks until planeSupportChannel has a Plane2DwSupport instance to transmit (i.e., its associated instance of SupportingPointFinder() has terminated)
			planeSupportStream <- planeSupport //blocks until planeSupportStream is ready to recieve a new value (i.e., its empty)
		}
	}()
	return planeSupportStream
}

// Reads Plane3DwSupport instances from input channel, and assigns the most well-supported plane to the given variable in memory
func DominantPlaneIdentifier(dominant *Plane3DwSupport, planeSupportStream <-chan Plane3DwSupport) {
	for planeSupport := range planeSupportStream { //range blocks until planeSupportStream has a new Plane3DwSupport instance to transmit and breaks when closed (i.e., all dominant plane candidates have been processed)
		if planeSupport.SupportSize > dominant.SupportSize {
			*dominant = planeSupport
		}
	}
}

// Runs the pipeline given a point cloud, number of iterations, and epsilon value; assigns the most well-supported plane to the given variable in memory
func run(points []Point3D, numIterations int, eps float64, dominant *Plane3DwSupport) {
	stop := make(chan bool)
	randomPointStream := RandomPointGenerator(points, stop)
	tripletStream := TripletGenerator(randomPointStream)
	nTripletStream := TakeN(numIterations, tripletStream, stop)
	planeStream := PlaneEstimator(nTripletStream)
	planeSupportChans := FanOut(points, eps, planeStream)
	planeSupportStream := FanIn(planeSupportChans)
	DominantPlaneIdentifier(dominant, planeSupportStream)
}

//**MAIN**//

func main() {

	// Thread-Count Optimization

	numThreads := 1362
	runtime.GOMAXPROCS(numThreads)
	fmt.Printf("Number of threads: %d \n", numThreads)
	start := time.Now() //for measuring run time

	// Read command line arguments

	// Program is executed using > go run planeRANSAC.go filename confidence percentage eps

	filename := os.Args[1] //os.Args[0] is the program file name!
	confidence, confidenceErr := strconv.ParseFloat(os.Args[2], 64)
	percentage, percentageErr := strconv.ParseFloat(os.Args[3], 64)
	eps, epsErr := strconv.ParseFloat(os.Args[4], 64)

	if confidenceErr != nil {
		fmt.Println("Error: could not parse confidence!")
	}
	if percentageErr != nil {
		fmt.Println("Error: could not parse percentage!")
	}
	if epsErr != nil {
		fmt.Println("Error: could not parse eps!")
	}
	if confidenceErr != nil || percentageErr != nil || epsErr != nil {
		fmt.Println("Exiting!")
		return
	}

	// Set up data required by plane RANSAC pipeline

	cloud := ReadXYZ(filename)
	numIterations := GetNumberOfIterations(confidence, percentage)
	dominantPlaneInliers := []Point3D{}
	filenameWithoutExtension := strings.TrimSuffix(filename, path.Ext(filename))

	// Loop to find and save three most dominant planes

	/*
		Note:
		Instead of manually running the program three times to find the three most dominant planes, and renaming the files,
		I have automated this step with a for loop! Everytime this program is run, all three dominant planes are found and
		saved to their respective files!
	*/

	for i := 0; i < 3; i++ {
		bestSupport := Plane3DwSupport{}
		run(cloud, numIterations, eps, &bestSupport)                                          //initiates pipeline to find and save the best supported (i.e., dominant) plane
		dominantPlaneInliers = GetSupportingPoints(bestSupport.Plane3D, cloud, eps)           //extract points from cloud that support dominant plane
		SaveXYZ(filenameWithoutExtension+"_p"+strconv.Itoa(i+1)+".xyz", dominantPlaneInliers) //save dominant plane inliers into new XYZ file
		cloud = RemovePlane(bestSupport.Plane3D, cloud, eps)                                  //remove dominant plane inliers from original point cloud -> refed into pipeline to find next most dominant plane
	}
	SaveXYZ(filenameWithoutExtension+"_p0.xyz", cloud) //save remaining points in cloud when inliers of three most dominant planes are removed

	// Measure and print runtime

	elapsed := time.Since(start)
	fmt.Printf("Run time: %s \n", elapsed)

}
